"""Constantes de l'intégration multisource_sensor."""

DOMAIN = "multisource_sensor"

# Clés de configuration YAML
CONF_PATTERN = "pattern"
CONF_TARGET_FORMAT = "target_format"
CONF_RECENCY_ATTR = "recency_attr"
CONF_BACKFILL = "backfill"
CONF_EXCLUDE = "exclude"
CONF_GROUPS = "groups"
CONF_NAME_FORMAT = "name_format"

# Valeurs par défaut
DEFAULT_RECENCY_ATTR = "last_updated"  # ou "last_changed"
DEFAULT_BACKFILL = "statistics"        # "statistics" | "none"

# Backfill : nombre de jours d'historique à reconstruire au démarrage
CONF_BACKFILL_DAYS = "backfill_days"
DEFAULT_BACKFILL_DAYS = 3650           # ~10 ans, borné par la rétention réelle des sources

# Attributs exposés par le capteur synthétique
ATTR_SOURCE = "source"            # entité retenue actuellement
ATTR_SOURCES = "sources"          # toutes les sources du groupe
ATTR_LAST_SOURCE_TS = "source_timestamp"

# États considérés comme invalides / à ignorer
INVALID_STATES = frozenset({"unknown", "unavailable", "none", "", None})

# Clés internes (hass.data) et persistance
DATA_COORDINATOR = "coordinator"
STORAGE_KEY = "multisource_sensor.signatures"
STORAGE_VERSION = 1

# Délai de regroupement des events registry (debounce) avant re-scan, en secondes.
# Un renommage en masse ou un rechargement d'intégration émet beaucoup d'events ;
# on attend que ça se calme avant de recalculer.
RESCAN_DEBOUNCE = 5.0
