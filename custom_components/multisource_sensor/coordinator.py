"""Coordinateur multisource_sensor.

Pièce centrale qui :
- détient les capteurs synthétiques vivants (par target_entity_id) ;
- recalcule les groupes à partir du registre d'entités, au démarrage puis à
  chaque event `entity_registry_updated` (création / suppression / renommage),
  avec un debounce pour absorber les rafales ;
- crée à la volée les nouveaux capteurs, met à jour à chaud la liste des sources
  des capteurs existants, et retire les capteurs dont le groupe a disparu ;
- ne déclenche le backfill que lorsque la **signature** du groupe (ensemble trié
  des sources) change, signature persistée dans un Store. Un verrou par cible
  évite deux backfills concurrents.

Modèle assumé : l'historique synthétique est une VUE RECALCULABLE des sources
actuelles. Quand une source disparaît, les heures concernées sont réattribuées à
la source suivante par le re-backfill complet.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field

from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.storage import Store

from .backfill import async_backfill_statistics
from .const import (
    DOMAIN,
    RESCAN_DEBOUNCE,
    STORAGE_KEY,
    STORAGE_VERSION,
)

_LOGGER = logging.getLogger(__name__)


@dataclass
class GroupSpec:
    """Spécification d'un groupe résolu à un instant t."""

    target_entity_id: str
    name: str
    sources: list[str] = field(default_factory=list)

    @property
    def signature(self) -> str:
        """Signature stable du groupe = ensemble trié des sources.

        Sert à décider si un re-backfill est nécessaire. Le nom et l'ordre
        d'apparition n'entrent pas dans la signature ; seul l'ensemble des
        sources compte (la fusion est commutative à l'ensemble près, la priorité
        étant réappliquée à chaque backfill via l'ordre courant).
        """
        return "|".join(sorted(self.sources))


class MultisourceCoordinator:
    """Gère le cycle de vie dynamique des capteurs synthétiques."""

    def __init__(
        self,
        hass: HomeAssistant,
        pattern: str | None,
        target_format: str,
        name_format: str,
        exclude: list[str],
        explicit_groups: list[dict],
        recency_attr: str,
        backfill_mode: str,
        backfill_days: int,
    ) -> None:
        self.hass = hass
        self.recency_attr = recency_attr
        self.backfill_mode = backfill_mode
        self.backfill_days = backfill_days

        self._rx = re.compile(pattern) if pattern else None
        self._target_format = target_format
        self._name_format = name_format
        self._exclude = set(exclude)
        self._explicit = explicit_groups

        # target_entity_id -> entité vivante (MultisourceSensor)
        self._entities: dict[str, "object"] = {}
        # target_entity_id -> verrou de backfill
        self._locks: dict[str, asyncio.Lock] = {}
        # callback d'ajout d'entités, fourni par la plateforme sensor
        self._async_add_entities: AddEntitiesCallback | None = None

        self._store: Store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        # target_entity_id -> signature déjà backfillée
        self._signatures: dict[str, str] = {}
        # cibles actuellement en conflit (entity_id déjà pris ailleurs) -> issue levée
        self._collisions: set[str] = set()

        self._unsub_registry = None
        self._debounce_cancel = None

    # --- Initialisation ------------------------------------------------------

    async def async_load(self) -> None:
        """Charge les signatures persistées."""
        data = await self._store.async_load()
        if isinstance(data, dict):
            self._signatures = dict(data.get("signatures", {}))

    async def _async_save(self) -> None:
        await self._store.async_save({"signatures": self._signatures})

    @callback
    def set_add_entities(self, async_add_entities: AddEntitiesCallback) -> None:
        """Fourni par la plateforme sensor pour pouvoir créer des entités à chaud."""
        self._async_add_entities = async_add_entities

    @callback
    def async_start(self) -> None:
        """Abonnement aux events du registre d'entités."""
        self._unsub_registry = self.hass.bus.async_listen(
            er.EVENT_ENTITY_REGISTRY_UPDATED, self._on_registry_updated
        )

    @callback
    def async_stop(self) -> None:
        if self._unsub_registry is not None:
            self._unsub_registry()
            self._unsub_registry = None
        if self._debounce_cancel is not None:
            self._debounce_cancel()
            self._debounce_cancel = None

    # --- Découverte ----------------------------------------------------------

    def discover(self) -> dict[str, GroupSpec]:
        """Reconstruit l'intégralité des groupes à partir de l'état courant.

        On scanne les entity_ids réellement présents dans la machine d'état (les
        sources doivent exister pour être agrégées). Les groupes explicites sont
        toujours présents même si certaines sources manquent.
        """
        groups: dict[str, GroupSpec] = {}

        # Groupes explicites
        for g in self._explicit:
            target = g["target"]
            name = g.get("name") or target.split(".", 1)[-1].replace("_", " ").title()
            groups[target] = GroupSpec(
                target_entity_id=target, name=name, sources=list(g["sources"])
            )

        # Auto-détection
        if self._rx is not None:
            for entity_id in self.hass.states.async_entity_ids("sensor"):
                if entity_id in self._exclude:
                    continue
                m = self._rx.match(entity_id)
                if not m:
                    continue
                captures = m.groupdict()
                try:
                    target = self._target_format.format(**captures)
                    display = self._name_format.format(
                        **{
                            k: (v or "").replace("_", " ").title()
                            for k, v in captures.items()
                        }
                    )
                except (KeyError, IndexError) as err:
                    _LOGGER.warning("Formatage cible impossible pour %s : %s", entity_id, err)
                    continue
                # Ne jamais s'auto-agréger : si une cible matche le pattern, on l'ignore.
                if entity_id == target:
                    continue
                grp = groups.get(target)
                if grp is None:
                    grp = GroupSpec(target_entity_id=target, name=display, sources=[])
                    groups[target] = grp
                if entity_id not in grp.sources:
                    grp.sources.append(entity_id)

        return groups

    # --- Réconciliation -------------------------------------------------------

    async def async_refresh(self) -> None:
        """Recalcule les groupes et réconcilie les entités vivantes.

        - nouveau groupe -> création d'entité + backfill ;
        - groupe existant dont les sources changent -> maj à chaud + backfill si
          la signature diffère de celle persistée ;
        - groupe disparu -> retrait de l'entité (et oubli de sa signature).
        """
        groups = self.discover()

        # 1. Suppressions : entités vivantes sans groupe correspondant.
        for target in list(self._entities):
            if target not in groups:
                entity = self._entities.pop(target)
                _LOGGER.info("multisource_sensor : retrait de %s (groupe disparu)", target)
                await entity.async_remove()
                self._signatures.pop(target, None)

        new_entities = []
        backfill_targets: list[GroupSpec] = []
        current_collisions: set[str] = set()

        # 2. Créations & mises à jour.
        for target, grp in groups.items():
            if not grp.sources:
                # Groupe vide (ex. explicite dont aucune source n'existe encore) :
                # on ne crée rien tant qu'il n'y a pas au moins une source.
                continue

            entity = self._entities.get(target)
            if entity is None:
                # La cible n'est pas (encore) à nous : refuser de l'écraser si un
                # autre composant occupe déjà cet entity_id. On lève une issue
                # Repairs et on n'instancie rien tant que le nom n'est pas libre.
                owner = self._target_collision(target)
                if owner is not None:
                    current_collisions.add(target)
                    self._async_raise_collision_issue(target, owner)
                    continue
                # Création différée : on instancie via la plateforme sensor.
                entity = self._build_entity(grp)
                self._entities[target] = entity
                new_entities.append(entity)
            else:
                # Mise à jour à chaud des sources si elles ont changé.
                entity.update_sources(grp.sources, grp.name)

            # Décision de backfill basée sur la signature.
            if (
                self.backfill_mode == "statistics"
                and self._signatures.get(target) != grp.signature
            ):
                backfill_targets.append(grp)

        # Lever les issues des conflits résolus (cible libérée, ou groupe disparu).
        for target in self._collisions - current_collisions:
            self._async_clear_collision_issue(target)
        self._collisions = current_collisions

        if new_entities and self._async_add_entities is not None:
            self._async_add_entities(new_entities)

        # 3. Backfills (après ajout, pour que l'unit soit résolu).
        for grp in backfill_targets:
            self.hass.async_create_task(self._async_backfill_group(grp))

    def _build_entity(self, grp: GroupSpec):
        """Instancie un capteur synthétique. Import différé pour éviter un cycle."""
        from .sensor import MultisourceSensor

        return MultisourceSensor(
            coordinator=self,
            target_entity_id=grp.target_entity_id,
            name=grp.name,
            sources=grp.sources,
            recency_attr=self.recency_attr,
        )

    # --- Détection de collision de cible -------------------------------------

    def _target_collision(self, target: str) -> str | None:
        """Renvoie l'origine occupant déjà `target`, ou None si la cible est libre.

        Une cible est « libre » si on la possède déjà, si rien ne l'occupe dans
        le registre d'entités, et si aucune entité hors-registre (template,
        legacy...) ne la publie dans la machine d'état. Sinon on renvoie le nom
        du composant en conflit pour l'afficher dans le diagnostic.
        """
        if target in self._entities:
            return None
        registry = er.async_get(self.hass)
        entry = registry.async_get(target)
        if entry is not None:
            # À nous (rechargement / redémarrage) -> pas un conflit.
            return None if entry.platform == DOMAIN else (entry.platform or "unknown")
        if self.hass.states.get(target) is not None:
            return "unknown"
        return None

    @staticmethod
    def _collision_issue_id(target: str) -> str:
        return f"target_collision_{target}"

    @callback
    def _async_raise_collision_issue(self, target: str, owner: str) -> None:
        _LOGGER.error(
            "multisource_sensor : cible %s déjà utilisée par '%s' ; capteur non créé",
            target,
            owner,
        )
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self._collision_issue_id(target),
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="target_collision",
            translation_placeholders={"target": target, "owner": owner},
        )

    @callback
    def _async_clear_collision_issue(self, target: str) -> None:
        ir.async_delete_issue(self.hass, DOMAIN, self._collision_issue_id(target))

    async def _async_backfill_group(self, grp: GroupSpec) -> None:
        """Backfill protégé par verrou + mise à jour de la signature persistée."""
        lock = self._locks.setdefault(grp.target_entity_id, asyncio.Lock())
        async with lock:
            # Re-vérification sous verrou : la signature a pu être traitée entre-temps.
            if self._signatures.get(grp.target_entity_id) == grp.signature:
                return
            entity = self._entities.get(grp.target_entity_id)
            unit = entity.resolved_unit if entity is not None else None
            await async_backfill_statistics(
                self.hass,
                target_entity_id=grp.target_entity_id,
                target_name=grp.name,
                sources=grp.sources,
                unit=unit,
                days=self.backfill_days,
            )
            self._signatures[grp.target_entity_id] = grp.signature
            await self._async_save()

    # --- Réaction aux events registry ----------------------------------------

    @callback
    def _on_registry_updated(self, event: Event) -> None:
        """Filtre les events pertinents puis planifie un re-scan debouncé."""
        action = event.data.get("action")
        if action not in ("create", "remove", "update"):
            return
        entity_id = event.data.get("entity_id", "")
        old_entity_id = event.data.get("old_entity_id")

        # Pertinent si l'entité (nouvelle ou ancienne) concerne un sensor matchant
        # le pattern, ou figure dans un groupe explicite, ou est une de nos cibles.
        if not self._is_relevant(entity_id) and not self._is_relevant(old_entity_id):
            return

        self._schedule_rescan()

    def _is_relevant(self, entity_id: str | None) -> bool:
        if not entity_id:
            return False
        if entity_id in self._entities:
            return True
        if self._rx is not None and self._rx.match(entity_id):
            return True
        for g in self._explicit:
            if entity_id in g["sources"] or entity_id == g["target"]:
                return True
        return False

    @callback
    def _schedule_rescan(self) -> None:
        if self._debounce_cancel is not None:
            self._debounce_cancel()

        async def _run(_now) -> None:
            self._debounce_cancel = None
            await self.async_refresh()

        self._debounce_cancel = async_call_later(self.hass, RESCAN_DEBOUNCE, _run)
