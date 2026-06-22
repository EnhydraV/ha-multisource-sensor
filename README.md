# multisource_sensor

A custom Home Assistant integration that creates **synthetic** sensors
aggregating several sources that report the same measurement (Bluetooth, cloud,
Matter, MQTT…). The synthetic sensor always reflects the **most recent value**,
and imports the **long-term history** (statistics) of its sources when it is
created.

## Installation

Copy the `custom_components/multisource_sensor` folder into Home Assistant's
`config/custom_components/` folder, then restart.

```
config/
└── custom_components/
    └── multisource_sensor/
        ├── __init__.py
        ├── backfill.py
        ├── const.py
        ├── manifest.json
        └── sensor.py
```

## Configuration

In `configuration.yaml`:

```yaml
multisource_sensor:
  # Auto-detection via a regex with named groups (zone, measure).
  pattern: '^sensor\.(?P<source>[^_]+)_(?P<zone>.+)_(?P<measure>.+)$'
  target_format: 'sensor.{zone}_{measure}'
  name_format: '{zone} {measure}'
  recency_attr: last_updated     # last_updated (recommended) | last_changed
  backfill: statistics           # statistics | none
  backfill_days: 3650            # upper bound; effectively capped by retention
  exclude:
    - sensor.source_a_garage_temperature   # entities to ignore
  # Explicit groups (optional, complement / take priority over the regex):
  groups:
    - target: sensor.cellar_temperature
      name: Cellar Temperature
      sources:
        - sensor.source_b_cellar_temperature
        - sensor.source_c_cellar_temperature
```

For example, these entities:

```
sensor.source_a_living_room_temperature   (matter)
sensor.source_b_living_room_temperature   (bluetooth)
sensor.source_c_living_room_temperature   (cloud)
sensor.source_d_living_room_temperature   (mqtt)
```

automatically produce `sensor.living_room_temperature`, which tracks the most
recent source and exposes as attributes: `source` (the selected entity),
`sources` (the group), and `source_timestamp`.

## How "most recent" works

On every state change of a source, `last_updated` (or `last_changed`) is
compared and the most recent value that is not `unavailable`/`unknown` is kept.

- `last_updated`: the last time the source **published** (even with an identical
  value). This is generally what you want for "the freshest measurement".
- `last_changed`: the last time the **value** actually changed.

## Dynamic discovery

Discovery is not limited to startup. The coordinator listens to
`entity_registry_updated` (creation, removal, rename). On every change affecting
a source matching the pattern, an explicit group, or an existing target, it
re-scans (with a few-seconds debounce to absorb bursts) and reconciles:

- **new source** → the synthetic sensor is created if it does not exist, or its
  source list is updated on the fly (re-subscription, without recreating the
  entity);
- **source removed / renamed** → the source is dropped from the group; if the
  group becomes empty, the sensor is removed;
- in both cases, if the group's **signature** (sorted set of sources) changes,
  the backfill is **replayed** for the affected target.

### Target collision protection

The integration never overwrites an existing entity. If a resolved target
`entity_id` is already owned by **another** component (entity registry or state
machine), the synthetic sensor is **not created**: an error is raised in
**Settings → Repairs** (issue registry) naming the conflicting origin, and the
sensor is created automatically once the `entity_id` becomes free (after fixing
`target_format` / the group `target`, or removing the conflicting entity). The
integration's own entities (across reloads and restarts) are not treated as
collisions.

## History backfill

For each group, the integration reads the **hourly statistics** already computed
by the recorder for each source (`mean`/`min`/`max`), **merges them hour by
hour** (the last source in the list wins on overlap), then imports the result
into the synthetic sensor via `async_import_statistics` — **the official API, no
raw SQL**.

The backfill is replayed only when a group's composition changes. The signature
of each already-backfilled group is persisted via the `Store` helper
(`.storage/multisource_sensor.signatures`), which avoids unnecessary
re-backfilling on every restart. A per-target lock prevents two concurrent
backfills.

**"Recomputable view" model.** The synthetic history is a view of the *current*
sources, not a frozen record. When a source disappears, the hours where it was
the most recent are **reassigned to the next source** during the full
re-backfill (the affected hours are rewritten). This is the intended and
accepted behavior.

### Important limitations (worth knowing)

- **Statistics, not fine-grained history.** Only the long-term curves
  ("statistics" graphs) are imported, not the raw `states` points of the last
  few days. The fine detail of the sources' past is not copied — a deliberate
  robustness trade-off.
- **Relies on stats already computed by the recorder.** The recorder only
  generates statistics for entities that have a `state_class` (`measurement`,
  etc.). Sources that never had hourly stats have nothing to import.
- **Measurement sensors only** (temperature, humidity, etc. → `has_mean`). For
  cumulative counters (`has_sum`, energy), mean/min/max merging is not suitable
  and sum alignment would need to be handled (not implemented).
- **The backfill replays when composition changes**, not on every startup: the
  persisted group signature short-circuits useless re-backfills.
  `async_import_statistics` is idempotent anyway (it rewrites the same hours), so
  it is harmless.
- **Display.** Imported data is visible through "Statistics" cards (statistic
  card, Plotly…). The recent "states" history remains the one accumulated by the
  synthetic sensor since its creation.

## Possible improvements

- Config flow (UI) instead of YAML.
- Support for counters (`has_sum`) with delta alignment.
- Configurable source-priority strategy (other than list order).
- Resuming history under the old `statistic_id` when a source is renamed
  (currently it re-merges from the current `statistic_id`s).
