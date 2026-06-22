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

## Pistes connues (non faites)

- Config flow (UI) au lieu du YAML.
- Support des compteurs (`has_sum`) avec alignement des deltas.
- Priorité de source paramétrable (autre que l'ordre de liste).
- Reprise d'historique sous l'ancien `statistic_id` lors d'un renommage.
