from abc import abstractmethod
from enum import Enum
from typing import Dict, Any, List
from datetime import datetime
from .plugin_base import BaseDataItem, BaseCollectorPlugin, BaseOutputPlugin

# Advisory-specific implementations
class AdvisoryStatus(Enum):
    """Enumeration for advisory status types."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class AdvisorySeverity(Enum):
    """Enumeration for advisory severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Advisory(BaseDataItem):
    """Advisory data item - inherits from BaseDataItem."""

    def __init__(self, advisory_id: str, title: str, description: str,
                 severity: AdvisorySeverity, source: str,
                 affected_areas: List[str] = None, created_at: datetime = None):
        super().__init__(advisory_id, "advisory", source, created_at)
        self.title = title
        self.description = description
        self.severity = severity
        self.status = AdvisoryStatus.ACTIVE
        self.affected_areas = affected_areas or []

    def validate(self) -> bool:
        """Validate advisory data."""
        return (self.title and self.description and
                self.severity and self.source)

    def to_dict(self) -> Dict[str, Any]:
        """Convert advisory to dictionary."""
        base_dict = {
            'item_id': self.item_id,
            'item_type': self.item_type,
            'source': self.source,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'metadata': self.metadata,
            'tags': self.tags
        }

        advisory_dict = {
            'title': self.title,
            'description': self.description,
            'severity': self.severity.value,
            'status': self.status.value,
            'affected_areas': self.affected_areas
        }

        return {**base_dict, **advisory_dict}

    def is_critical(self) -> bool:
        """Check if advisory is critical severity."""
        return self.severity == AdvisorySeverity.CRITICAL

    def is_active(self) -> bool:
        """Check if advisory is active."""
        return self.status == AdvisoryStatus.ACTIVE


# Advisory-specific plugin base classes
class AdvisoryCollectorPlugin(BaseCollectorPlugin[Advisory]):
    """Base class for advisory collection plugins."""

    def get_data_type(self) -> str:
        return "advisory"

    @abstractmethod
    def collect_advisories(self) -> List[Advisory]:
        """Collect advisories from the data source."""
        pass

    def collect_data(self) -> List[Advisory]:
        """Implementation of generic collect_data method."""
        return self.collect_advisories()


class AdvisoryOutputPlugin(BaseOutputPlugin[Advisory]):
    """Base class for advisory output plugins."""

    def get_supported_data_types(self) -> List[str]:
        return ["advisory"]

    @abstractmethod
    def send_advisory(self, advisory: Advisory) -> bool:
        """Send advisory to the output destination."""
        pass

    def send_data(self, data_item: Advisory) -> bool:
        """Implementation of generic send_data method."""
        return self.send_advisory(data_item)
