"""Plateforme sensor pour multisource_sensor.

L'entité reflète la source la plus récente de son groupe et supporte la mise à
jour de sa liste de sources à chaud (réabonnement aux nouvelles entités). Le
backfill n'est pas déclenché ici : il est piloté par le coordinateur en fonction
de la signature du groupe.
"""
from __future__ import annotations

import logging

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import ATTR_DEVICE_CLASS, ATTR_UNIT_OF_MEASUREMENT
from homeassistant.core import Event, HomeAssistant, State, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.util import dt as dt_util

from .const import (
    ATTR_LAST_SOURCE_TS,
    ATTR_SOURCE,
    ATTR_SOURCES,
    DATA_COORDINATOR,
    DOMAIN,
    INVALID_STATES,
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Branche le coordinateur sur la plateforme et lance la première découverte."""
    if discovery_info is None:
        return

    coordinator = hass.data[DOMAIN][DATA_COORDINATOR]
    coordinator.set_add_entities(async_add_entities)
    coordinator.async_start()
    # Première réconciliation : crée les entités initiales + backfills nécessaires.
    await coordinator.async_refresh()


class MultisourceSensor(SensorEntity):
    """Capteur qui reflète la source la plus récente parmi un groupe."""

    _attr_should_poll = False

    def __init__(
        self,
        coordinator,
        target_entity_id: str,
        name: str,
        sources: list[str],
        recency_attr: str,
    ) -> None:
        self._coordinator = coordinator
        self.entity_id = target_entity_id
        self._attr_name = name
        self._attr_unique_id = target_entity_id
        self._sources = list(sources)
        self._recency_attr = recency_attr

        self._attr_native_value = None
        self._current_source: str | None = None
        self._current_source_ts = None
        self._unsub = None

    # --- Sélection de la valeur la plus récente ------------------------------

    def _recency_ts(self, state: State) -> float:
        ref = (
            state.last_updated
            if self._recency_attr == "last_updated"
            else state.last_changed
        )
        return ref.timestamp() if ref else 0.0

    @callback
    def _recompute(self) -> None:
        best_state: State | None = None
        best_ts = -1.0
        for src in self._sources:
            st = self.hass.states.get(src)
            if st is None or st.state in INVALID_STATES:
                continue
            ts = self._recency_ts(st)
            if ts > best_ts:
                best_ts = ts
                best_state = st

        if best_state is None:
            self._attr_native_value = None
            self._current_source = None
            self._current_source_ts = None
            return

        self._attr_native_value = best_state.state
        self._current_source = best_state.entity_id
        self._current_source_ts = dt_util.utc_from_timestamp(best_ts)

        unit = best_state.attributes.get(ATTR_UNIT_OF_MEASUREMENT)
        if unit is not None:
            self._attr_native_unit_of_measurement = unit
        dev_class = best_state.attributes.get(ATTR_DEVICE_CLASS)
        if dev_class is not None:
            try:
                self._attr_device_class = SensorDeviceClass(dev_class)
            except ValueError:
                pass
        if self._attr_state_class is None:
            self._attr_state_class = SensorStateClass.MEASUREMENT

    @property
    def resolved_unit(self) -> str | None:
        """Unité retenue (utilisée par le coordinateur pour le backfill)."""
        return self._attr_native_unit_of_measurement

    # --- Mise à jour des sources à chaud -------------------------------------

    @callback
    def update_sources(self, sources: list[str], name: str | None = None) -> None:
        """Remplace la liste de sources et réabonne, sans recréer l'entité."""
        new_sources = list(sources)
        if new_sources == self._sources and (name is None or name == self._attr_name):
            return
        self._sources = new_sources
        if name:
            self._attr_name = name
        if self.hass is not None:
            self._resubscribe()
            self._recompute()
            self.async_write_ha_state()

    @callback
    def _resubscribe(self) -> None:
        if self._unsub is not None:
            self._unsub()
            self._unsub = None

        @callback
        def _source_changed(event: Event) -> None:
            self._recompute()
            self.async_write_ha_state()

        self._unsub = async_track_state_change_event(
            self.hass, self._sources, _source_changed
        )

    # --- Cycle de vie --------------------------------------------------------

    async def async_added_to_hass(self) -> None:
        self._resubscribe()
        self._recompute()

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub is not None:
            self._unsub()
            self._unsub = None

    # --- Attributs -----------------------------------------------------------

    @property
    def extra_state_attributes(self) -> dict:
        return {
            ATTR_SOURCE: self._current_source,
            ATTR_SOURCES: self._sources,
            ATTR_LAST_SOURCE_TS: self._current_source_ts.isoformat()
            if self._current_source_ts
            else None,
        }

    @property
    def available(self) -> bool:
        return self._attr_native_value is not None
