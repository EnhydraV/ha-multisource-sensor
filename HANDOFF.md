# HANDOFF — multisource_sensor

Intégration Home Assistant (YAML + discovery, pas de config flow) qui crée des
capteurs synthétiques agrégeant plusieurs sources (la valeur la plus récente
l'emporte) et backfill l'historique long terme via `async_import_statistics`.

## Fichiers

- `__init__.py` — `async_setup` depuis `configuration.yaml`, instancie le
  coordinateur, charge la plateforme sensor.
- `coordinator.py` — cœur : découverte (regex + groupes explicites),
  réconciliation à chaud sur `entity_registry_updated` (debounce), décision de
  backfill par signature de groupe (persistée via `Store`).
- `sensor.py` — `MultisourceSensor` : reflète la source la plus récente, maj des
  sources à chaud sans recréer l'entité.
- `backfill.py` — fusion heure par heure des statistics des sources.
- `const.py`, `manifest.json`.
- `strings.json` + `translations/{en,fr}.json` — libellés des issues Repairs.

## 2026-06-22

- README réécrit en anglais et rendu générique (suppression des conventions de
  nommage maison `sb*`).
- **Protection contre la collision de cible** : avant de créer un capteur
  synthétique, `coordinator._target_collision()` vérifie que l'`entity_id` cible
  n'est pas déjà occupé par une autre origine (registre ou machine d'état). En
  cas de conflit, le capteur n'est **pas** créé et une issue Repairs
  (`issue_registry`, severity ERROR, `translation_key="target_collision"`) est
  levée. L'issue est effacée automatiquement quand le conflit disparaît (cible
  libérée ou groupe disparu) — suivi via `self._collisions`. Nos propres entités
  (rechargement/redémarrage, `entry.platform == DOMAIN`) ne sont pas considérées
  comme un conflit.

- **Mise en conformité structure HACS** : tout le code de l'intégration
  (`__init__.py`, `coordinator.py`, `sensor.py`, `backfill.py`, `const.py`,
  `manifest.json`, `strings.json`, `translations/`) a été déplacé de la racine
  vers `custom_components/multisource_sensor/` via `git mv`. Ajout d'un
  `hacs.json` (name + render_readme) et d'un `.gitignore` à la racine. HACS
  rejetait le dépôt tant que `custom_components/*/manifest.json` n'existait pas.

- **Fix crash `_recompute` (AttributeError state_class)** : `sensor.py` lisait
  `self._attr_state_class` avant toute écriture ; selon la version HA ce backing
  attribute n'a pas de défaut lisible → `AttributeError` (47 occurrences en logs).
  `state_class` est désormais un attribut de classe
  (`_attr_state_class = SensorStateClass.MEASUREMENT`) et la lecture conditionnelle
  a été supprimée de `_recompute`.
- **Dépréciation `has_mean` → `mean_type`** : `backfill.py` construit la metadata
  avec `mean_type=StatisticMeanType.ARITHMETIC` (import gardé : fallback `has_mean`
  si HA < 2025.5). `has_mean` est retiré côté HA en 2026.4.

- **Fix crash `resolved_unit` (AttributeError native_unit)** : même motif que
  state_class. `_attr_native_unit_of_measurement` (et `_attr_device_class`)
  n'étaient écrits que conditionnellement dans `_recompute` ; désormais
  initialisés à None dans `__init__`.
- **Dépréciation `unit_class`** : `backfill.py` ajoute `unit_class` à la metadata
  via `_unit_class()` (lookup `STATISTIC_UNIT_TO_UNIT_CONVERTER`, None si pas de
  convertisseur). Requis par `async_import_statistics` dès HA 2025.11.
- **`exclude` accepte les regex** : chaque entrée de `exclude` est compilée en
  regex et testée en `fullmatch` (`coordinator._is_excluded`) ; entrée invalide
  → comparaison littérale + warning. Schéma assoupli `cv.entity_id` → `cv.string`
  dans `__init__.py`. Rétrocompatible (un entity_id exact = fullmatch de lui-même).

- **Synchronisation de la pièce (area)** : le capteur synthétique hérite de la
  pièce de ses sources via `coordinator.async_sync_area`. Mode permissif : on
  prend la pièce de la **1re source** (ordre de priorité) qui en a une ; si
  aucune n'en a, on ne touche à rien (jamais d'effacement). Synchro forcée : un
  area_id manuel est réécrasé dès qu'une source en a une. Pièce effective d'une
  source = son `area_id`, sinon celui de son `device` (`_source_area`). Appelée à
  chaque réconciliation pour les cibles possédées, et dans
  `MultisourceSensor.async_added_to_hass` pour l'entité fraîchement créée (pas
  encore dans le registre lors de la passe).

## Pistes connues (non faites)

- Config flow (UI) au lieu du YAML.
- Support des compteurs (`has_sum`) avec alignement des deltas.
- Priorité de source paramétrable (autre que l'ordre de liste).
- Reprise d'historique sous l'ancien `statistic_id` lors d'un renommage.
