"""Intégration multisource_sensor.

Crée des capteurs synthétiques qui agrègent plusieurs sources fournissant la
même mesure (bluetooth, cloud, matter, mqtt...) et exposent toujours la valeur
la plus récente. La découverte est dynamique : ajout, suppression et renommage
de sources sont pris en compte à chaud, et le backfill de l'historique long
terme (statistics) est rejoué lorsque la composition d'un groupe change.
"""
from __future__ import annotations

import logging

import voluptuous as vol

from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.discovery import async_load_platform
from homeassistant.helpers.typing import ConfigType

from .const import (
    CONF_BACKFILL,
    CONF_BACKFILL_DAYS,
    CONF_EXCLUDE,
    CONF_GROUPS,
    CONF_NAME_FORMAT,
    CONF_PATTERN,
    CONF_RECENCY_ATTR,
    CONF_TARGET_FORMAT,
    DATA_COORDINATOR,
    DEFAULT_BACKFILL,
    DEFAULT_BACKFILL_DAYS,
    DEFAULT_RECENCY_ATTR,
    DOMAIN,
    SERVICE_BACKFILL_HELPER,
)
from .coordinator import MultisourceCoordinator
from .helper_backfill import async_backfill_helper

BACKFILL_HELPER_SCHEMA = vol.Schema(
    {
        vol.Required("entity_id"): cv.entity_ids,
        vol.Optional("days", default=DEFAULT_BACKFILL_DAYS): cv.positive_int,
    }
)

_LOGGER = logging.getLogger(__name__)

GROUP_SCHEMA = vol.Schema(
    {
        vol.Required("target"): cv.entity_id,
        vol.Required("sources"): vol.All(cv.ensure_list, [cv.entity_id]),
        vol.Optional("name"): cv.string,
    }
)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_PATTERN): cv.string,
                vol.Optional(
                    CONF_TARGET_FORMAT, default="sensor.sb_{zone}_{measure}"
                ): cv.string,
                vol.Optional(
                    CONF_NAME_FORMAT, default="SB {zone} {measure}"
                ): cv.string,
                vol.Optional(
                    CONF_RECENCY_ATTR, default=DEFAULT_RECENCY_ATTR
                ): vol.In(["last_updated", "last_changed"]),
                vol.Optional(CONF_BACKFILL, default=DEFAULT_BACKFILL): vol.In(
                    ["statistics", "none"]
                ),
                vol.Optional(
                    CONF_BACKFILL_DAYS, default=DEFAULT_BACKFILL_DAYS
                ): cv.positive_int,
                # Chaque entrée est une regex (fullmatch) ou un entity_id littéral.
                vol.Optional(CONF_EXCLUDE, default=[]): vol.All(
                    cv.ensure_list, [cv.string]
                ),
                vol.Optional(CONF_GROUPS, default=[]): vol.All(
                    cv.ensure_list, [GROUP_SCHEMA]
                ),
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Mise en place depuis configuration.yaml."""
    conf = config.get(DOMAIN)
    if conf is None:
        return True

    coordinator = MultisourceCoordinator(
        hass,
        pattern=conf.get(CONF_PATTERN),
        target_format=conf[CONF_TARGET_FORMAT],
        name_format=conf[CONF_NAME_FORMAT],
        exclude=conf[CONF_EXCLUDE],
        explicit_groups=conf[CONF_GROUPS],
        recency_attr=conf[CONF_RECENCY_ATTR],
        backfill_mode=conf[CONF_BACKFILL],
        backfill_days=conf[CONF_BACKFILL_DAYS],
    )
    await coordinator.async_load()

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][DATA_COORDINATOR] = coordinator

    async def _handle_backfill_helper(call) -> None:
        """Reconstruit l'historique des group sensors ciblés (min/max/mean)."""
        days = call.data["days"]
        for entity_id in call.data["entity_id"]:
            await async_backfill_helper(hass, entity_id, days)

    hass.services.async_register(
        DOMAIN,
        SERVICE_BACKFILL_HELPER,
        _handle_backfill_helper,
        schema=BACKFILL_HELPER_SCHEMA,
    )

    # Arrêt propre des abonnements.
    hass.bus.async_listen_once(
        EVENT_HOMEASSISTANT_STOP, lambda _e: coordinator.async_stop()
    )

    # La plateforme sensor récupère le coordinateur via hass.data.
    hass.async_create_task(
        async_load_platform(hass, Platform.SENSOR, DOMAIN, {}, config)
    )
    return True
