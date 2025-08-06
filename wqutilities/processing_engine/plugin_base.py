from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, List, Generic, TypeVar
from enum import Enum
import json
import logging
from dataclasses import dataclass

# Generic type variable for data items
T = TypeVar('T')


class PluginStatus(Enum):
    """Enumeration for plugin status."""
    ENABLED = "enabled"
    DISABLED = "disabled"
    ERROR = "error"


@dataclass
class PluginConfig:
    """Configuration data for plugins."""
    name: str = ""
    module: str = ""
    enabled: bool = True
    config: Dict[str, Any] = None
    retry_count: int = 3
    timeout: int = 30

    def __post_init__(self):
        if self.config is None:
            self.config = {}

@dataclass
class BaseDataItem(ABC):
    """Base class for all data items that can be processed by the engine."""
    item_id: str
    item_type: str
    source: str
    created_at: datetime
    updated_at: datetime
    metadata:  {}
    tags: []

    @abstractmethod
    def validate(self) -> bool:
        """Validate the data item."""
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert data item to dictionary representation."""
        pass
    @abstractmethod
    def get_record(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_record_datetime(self) -> datetime:
        pass
    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata to the data item."""
        self.metadata[key] = value
        self.updated_at = datetime.now()

    def add_tag(self, tag: str) -> None:
        """Add a tag to the data item."""
        if tag not in self.tags:
            self.tags.append(tag)
            self.updated_at = datetime.now()

    def remove_tag(self, tag: str) -> None:
        """Remove a tag from the data item."""
        if tag in self.tags:
            self.tags.remove(tag)
            self.updated_at = datetime.now()

    def get_age_in_hours(self) -> float:
        """Get the age of the data item in hours."""
        return (datetime.now() - self.created_at).total_seconds() / 3600

    def to_json(self) -> str:
        """Convert data item to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class BaseCollectorPlugin(ABC, Generic[T]):
    """Base class for data collection plugins."""

    def __init__(self, config: PluginConfig):
        self.plugin_config = config
        self.status = PluginStatus.ENABLED if config.enabled else PluginStatus.DISABLED
        self.logger = logging.getLogger(f"collector.{config.name}")
        self.last_run = None
        self.error_count = 0

    @abstractmethod
    def collect_data(self) -> List[T]:
        """Collect data items from the data source."""
        pass


    @abstractmethod
    def get_data_type(self) -> str:
        """Return the type of data this plugin collects."""
        pass

    def validate_config(self) -> bool:
        if len(self.plugin_config.name) and len(self.plugin_config.module):
            return True
        return False

    def get_plugin_name(self) -> str:
        return self.plugin_config.module

    def is_enabled(self) -> bool:
        """Check if plugin is enabled."""
        return self.status == PluginStatus.ENABLED

    def set_status(self, status: PluginStatus):
        """Set plugin status."""
        self.status = status
        self.logger.info(f"Plugin status changed to: {status.value}")

    def handle_error(self, error: Exception):
        """Handle plugin errors."""
        self.error_count += 1
        self.logger.error(f"Plugin error (count: {self.error_count}): {str(error)}")
        if self.error_count >= self.plugin_config.retry_count:
            self.set_status(PluginStatus.ERROR)


class BaseOutputPlugin(ABC, Generic[T]):
    """Base class for data output plugins."""

    def __init__(self, config: PluginConfig):
        self.plugin_config = config
        self.status = PluginStatus.ENABLED if config.enabled else PluginStatus.DISABLED
        self.logger = logging.getLogger(f"output.{config.name}")
        self.sent_count = 0
        self.error_count = 0

    @abstractmethod
    def get_plugin_name(self) -> str:
        """Return the plugin name."""
        pass

    @abstractmethod
    def send_data(self, data_item: T) -> bool:
        """Send data item to the output destination."""
        pass

    @abstractmethod
    def validate_config(self) -> bool:
        """Validate plugin configuration."""
        pass

    @abstractmethod
    def format_message(self, data_item: T) -> str:
        """Format data item message for this output type."""
        pass

    @abstractmethod
    def get_supported_data_types(self) -> List[str]:
        """Return list of data types this plugin can handle."""
        pass

    def is_enabled(self) -> bool:
        """Check if plugin is enabled."""
        return self.status == PluginStatus.ENABLED

    def should_send(self, data_item: T) -> bool:
        """Determine if data item should be sent via this plugin."""
        # Override in subclasses for filtering logic
        return data_item.item_type in self.get_supported_data_types()

    def set_status(self, status: PluginStatus):
        """Set plugin status."""
        self.status = status
        self.logger.info(f"Plugin status changed to: {status.value}")

    def handle_error(self, error: Exception):
        """Handle plugin errors."""
        self.error_count += 1
        self.logger.error(f"Plugin error (count: {self.error_count}): {str(error)}")
        if self.error_count >= self.plugin_config.retry_count:
            self.set_status(PluginStatus.ERROR)
