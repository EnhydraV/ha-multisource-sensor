"""Backfill de l'historique long terme (statistics) du capteur synthétique.

Stratégie : pour chaque source, on récupère les statistiques horaires déjà
calculées par le recorder (mean/min/max). On fusionne heure par heure : pour une
heure donnée, on retient la valeur de la source qui possède une donnée — en cas
de chevauchement, on privilégie la dernière source de la liste de priorité (par
défaut l'ordre des sources tel que fourni). Puis on importe le résultat dans le
statistic_id du capteur synthétique via async_import_statistics.

On n'écrit JAMAIS de SQL brut : tout passe par l'API officielle du recorder,
ce qui évite la corruption et survit aux purges/migrations.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    statistics_during_period,
)
from homeassistant.components.sensor import SensorDeviceClass
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

_LOGGER = logging.getLogger(__name__)


async def async_backfill_statistics(
    hass: HomeAssistant,
    target_entity_id: str,
    target_name: str,
    sources: list[str],
    unit: str | None,
    days: int,
) -> None:
    """Fusionne et importe l'historique statistics des sources dans la cible.

    `sources` est ordonné par priorité croissante : en cas d'égalité sur une
    heure, la source la plus à droite (dernière) gagne. On considère que la
    source listée en dernier est la plus fiable / la plus « récente » par
    convention de l'appelant. Pour un capteur de mesure, on agrège mean/min/max.
    """
    if not sources:
        return

    recorder = get_instance(hass)
    end = dt_util.utcnow()
    start = end - timedelta(days=days)

    # Récupération des statistiques horaires de toutes les sources en un appel.
    stats = await recorder.async_add_executor_job(
        _get_period,
        hass,
        start,
        end,
        set(sources),
    )

    if not stats:
        _LOGGER.info(
            "multisource_sensor : aucune statistique source à reprendre pour %s",
            target_entity_id,
        )
        return

    # Fusion heure par heure. Clé = start (datetime aligné à l'heure).
    merged: dict[datetime, dict] = {}

    # On parcourt les sources dans l'ordre de priorité croissante : les écritures
    # ultérieures écrasent les précédentes, donc la dernière source gagne.
    for src in sources:
        rows = stats.get(src)
        if not rows:
            continue
        for row in rows:
            # row["start"] peut être un timestamp (float) selon la version HA.
            raw_start = row["start"]
            if isinstance(raw_start, (int, float)):
                hour = dt_util.utc_from_timestamp(raw_start)
            else:
                hour = dt_util.as_utc(raw_start)
            # Alignement strict à l'heure (sécurité).
            hour = hour.replace(minute=0, second=0, microsecond=0)

            mean = row.get("mean")
            if mean is None:
                # Sans moyenne, la ligne n'est pas exploitable pour une mesure.
                continue

            merged[hour] = {
                "start": hour,
                "mean": mean,
                "min": row.get("min", mean),
                "max": row.get("max", mean),
            }

    if not merged:
        _LOGGER.info(
            "multisource_sensor : fusion vide pour %s, rien à importer",
            target_entity_id,
        )
        return

    statistics = [merged[h] for h in sorted(merged)]

    metadata = {
        "has_mean": True,
        "has_sum": False,
        "name": target_name,
        "source": "recorder",          # statistique « interne » liée à une entité
        "statistic_id": target_entity_id,
        "unit_of_measurement": unit,
    }

    _LOGGER.info(
        "multisource_sensor : import de %d points horaires dans %s",
        len(statistics),
        target_entity_id,
    )
    # async_import_statistics planifie l'écriture dans le thread du recorder.
    async_import_statistics(hass, metadata, statistics)


def _get_period(
    hass: HomeAssistant,
    start: datetime,
    end: datetime,
    statistic_ids: set[str],
) -> dict:
    """Exécuté dans le thread du recorder : lit les stats horaires des sources."""
    return statistics_during_period(
        hass,
        start,
        end,
        statistic_ids,
        "hour",
        None,
        {"mean", "min", "max"},
    )
