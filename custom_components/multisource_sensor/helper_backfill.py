"""Backfill de l'historique (statistics) des helpers « group sensor ».

Service `multisource_sensor.backfill_helper` : reconstruit l'historique long
terme d'un helper « Combine the state of several sensors » (type min, max ou
mean) à partir des stats horaires de ses membres, en appliquant l'algorithme du
group heure par heure. Chaque appel ÉCRASE puis remplace l'historique existant.

Approximation assumée (voir README) : seule la moyenne du type `mean` est exacte
(linéarité). Les min/max reconstruits sont des bornes, pas le vrai min/max
instantané de la combinaison.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    get_metadata,
)
from homeassistant.const import (
    ATTR_UNIT_OF_MEASUREMENT,
    CONF_ENTITIES,
    CONF_TYPE,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from .backfill import _HAS_MEAN_TYPE, _get_period, _unit_class

if _HAS_MEAN_TYPE:
    from homeassistant.components.recorder.models import StatisticMeanType

_LOGGER = logging.getLogger(__name__)

# Types de combinaison supportés -> réduction appliquée à une liste de valeurs.
_REDUCERS = {
    "min": min,
    "max": max,
    "mean": lambda values: sum(values) / len(values),
}

# Clés possibles pour la liste des membres, selon l'intégration backing le helper
# (group : "entities" ; min_max : "entity_ids" ; etc.). Première clé non vide gagne.
_MEMBER_KEYS = (CONF_ENTITIES, "entity_ids", "members")


async def async_backfill_helper(
    hass: HomeAssistant,
    helper_entity_id: str,
    days: int,
) -> None:
    """Reconstruit et remplace l'historique statistics d'un group sensor."""
    spec = _resolve_group(hass, helper_entity_id)
    if spec is None:
        return
    members, ctype = spec
    reducer = _REDUCERS[ctype]

    recorder = get_instance(hass)
    end = dt_util.utcnow()
    start = end - timedelta(days=days)

    stats = await recorder.async_add_executor_job(
        _get_period, hass, start, end, set(members)
    )
    if not stats:
        _LOGGER.warning(
            "backfill_helper : aucune statistique source pour %s", helper_entity_id
        )
        return

    series = _reconstruct(members, stats, reducer)
    if not series:
        _LOGGER.warning(
            "backfill_helper : rien à reconstruire pour %s", helper_entity_id
        )
        return

    metadata = await recorder.async_add_executor_job(
        _build_metadata, hass, helper_entity_id
    )

    # Remplacement complet : on efface tout, puis on réimporte. Les deux passent
    # par la file de tâches du recorder (thread unique), dans cet ordre.
    recorder.async_clear_statistics([helper_entity_id])
    _LOGGER.info(
        "backfill_helper : %s reconstruit (type %s) — %d points horaires",
        helper_entity_id,
        ctype,
        len(series),
    )
    async_import_statistics(hass, metadata, series)


def _resolve_group(
    hass: HomeAssistant, helper_entity_id: str
) -> tuple[list[str], str] | None:
    """Renvoie (membres, type) du group sensor, ou None si invalide/non supporté."""
    ent_reg = er.async_get(hass)
    entry = ent_reg.async_get(helper_entity_id)
    if entry is None or entry.config_entry_id is None:
        _LOGGER.error(
            "backfill_helper : %s introuvable dans le registre (ou sans config entry)",
            helper_entity_id,
        )
        return None
    config_entry = hass.config_entries.async_get_entry(entry.config_entry_id)
    if config_entry is None:
        _LOGGER.error(
            "backfill_helper : config entry introuvable pour %s", helper_entity_id
        )
        return None

    # On ne se fie pas au domaine (group, helper « combine »… selon les versions)
    # mais à la FORME de la config : un type min/max/mean + une liste de membres.
    # data et options sont fusionnés car les intégrations rangent l'un ou l'autre.
    conf = {**config_entry.data, **config_entry.options}
    ctype = conf.get(CONF_TYPE)
    members = next((conf[k] for k in _MEMBER_KEYS if conf.get(k)), None)

    if ctype not in _REDUCERS or not members:
        _LOGGER.error(
            "backfill_helper : %s (intégration « %s ») n'est pas un helper combine "
            "min/max/mean exploitable — type=%r, clés de config dispo=%s",
            helper_entity_id,
            config_entry.domain,
            ctype,
            sorted(conf.keys()),
        )
        return None

    if config_entry.domain != "group":
        _LOGGER.info(
            "backfill_helper : %s provient de « %s » (pas « group ») — pris en "
            "charge via type=%s, %d membre(s)",
            helper_entity_id,
            config_entry.domain,
            ctype,
            len(members),
        )
    return list(members), ctype


def _reconstruct(members: list[str], stats: dict, reducer) -> list[dict]:
    """Applique `reducer` heure par heure sur les stats horaires des membres."""
    # hour -> {"mean": [...], "min": [...], "max": [...]} pour les membres présents.
    by_hour: dict[datetime, dict[str, list[float]]] = {}
    for member in members:
        for row in stats.get(member, []):
            mean = row.get("mean")
            if mean is None:
                continue
            agg = by_hour.setdefault(
                _row_hour(row), {"mean": [], "min": [], "max": []}
            )
            agg["mean"].append(mean)
            agg["min"].append(row.get("min", mean))
            agg["max"].append(row.get("max", mean))

    series = []
    for hour in sorted(by_hour):
        agg = by_hour[hour]
        if not agg["mean"]:
            continue
        series.append(
            {
                "start": hour,
                "mean": reducer(agg["mean"]),
                "min": reducer(agg["min"]),
                "max": reducer(agg["max"]),
            }
        )
    return series


def _build_metadata(hass: HomeAssistant, helper_entity_id: str) -> dict:
    """Réutilise la metadata existante du helper, ou la construit par défaut."""
    existing = get_metadata(hass, statistic_ids={helper_entity_id})
    if helper_entity_id in existing:
        meta = dict(existing[helper_entity_id][1])
        # On force l'identité ; le reste (mean_type, unit_class, has_sum…) est repris.
        meta["statistic_id"] = helper_entity_id
        meta["source"] = "recorder"
        return meta

    # Le helper n'a encore aucune statistique : metadata minimale.
    state = hass.states.get(helper_entity_id)
    unit = state.attributes.get(ATTR_UNIT_OF_MEASUREMENT) if state else None
    meta = {
        "has_sum": False,
        "name": state.name if state else None,
        "source": "recorder",
        "statistic_id": helper_entity_id,
        "unit_class": _unit_class(unit),
        "unit_of_measurement": unit,
    }
    if _HAS_MEAN_TYPE:
        meta["mean_type"] = StatisticMeanType.ARITHMETIC
    else:
        meta["has_mean"] = True
    return meta


def _row_hour(row) -> datetime:
    """Heure UTC alignée d'une ligne de statistique (start peut être un float)."""
    raw = row["start"]
    if isinstance(raw, (int, float)):
        hour = dt_util.utc_from_timestamp(raw)
    else:
        hour = dt_util.as_utc(raw)
    return hour.replace(minute=0, second=0, microsecond=0)
