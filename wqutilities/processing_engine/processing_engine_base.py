from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, List, Callable, Generic
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib.util
import inspect
from pathlib import Path

from .plugin_loader import PluginLoader
from .plugin_base import BaseCollectorPlugin, BaseOutputPlugin, T, PluginConfig
from .base_advisory import Advisory

class GenericProcessingEngine(Generic[T]):
    """Generic processing engine for any data type with plugin management."""

    def __init__(self, max_workers: int = 5, plugin_dirs: Dict[str, str] = None,
                 config_dirs: List[str] = None):
      self.collector_plugins: Dict[str, BaseCollectorPlugin[T]] = {}
      self.output_plugins: Dict[str, BaseOutputPlugin[T]] = {}
      self.data_items: Dict[str, T] = {}
      self.max_workers = max_workers
      self.logger = logging.getLogger("processing_engine")
      self.running = False
      self.filters: List[Callable[[T], bool]] = []
      self.processors: List[Callable[[T], T]] = []

      # Plugin directories
      self.plugin_dirs = plugin_dirs or {
        'collectors': './plugins/collectors',
        'outputs': './plugins/outputs'
      }

      # Config directories - can include plugin directories and additional paths
      additional_config_dirs = config_dirs or ['./configs']
      self.config_dirs = PluginLoader.find_all_config_directories(
        self.plugin_dirs, additional_config_dirs
      )

      # Load plugins and configurations
      self.plugin_configs = PluginLoader.load_plugin_configs(self.config_dirs)
      self.auto_load_plugins()

    def auto_load_plugins(self):
      """Automatically load all plugins from configured directories."""
      self.logger.info("Auto-loading plugins from directories")

      # Load collector plugins
      collector_classes = PluginLoader.load_plugins_from_directory(
        self.plugin_dirs['collectors'], BaseCollectorPlugin
      )

      for class_name, plugin_class in collector_classes.items():
        try:
          # Get configuration for this plugin
          config_name = class_name.lower().replace('collector', '').replace('plugin', '')
          config = self.plugin_configs.get(config_name, PluginConfig(config_name))

          # Create and register plugin instance
          plugin_instance = plugin_class(config)
          self.register_collector_plugin(plugin_instance)

        except Exception as e:
          self.logger.error(f"Failed to instantiate collector plugin {class_name}: {str(e)}")

      # Load output plugins
      output_classes = PluginLoader.load_plugins_from_directory(
        self.plugin_dirs['outputs'], BaseOutputPlugin
      )

      for class_name, plugin_class in output_classes.items():
        try:
          # Get configuration for this plugin
          config_name = class_name.lower().replace('output', '').replace('plugin', '')
          config = self.plugin_configs.get(config_name, PluginConfig(config_name))

          # Create and register plugin instance
          plugin_instance = plugin_class(config)
          self.register_output_plugin(plugin_instance)

        except Exception as e:
          self.logger.error(f"Failed to instantiate output plugin {class_name}: {str(e)}")

    def register_collector_plugin(self, plugin: BaseCollectorPlugin[T]):
      """Register a collector plugin."""
      if not plugin.validate_config():
        raise ValueError(f"Invalid configuration for plugin: {plugin.get_plugin_name()}")

      plugin_name = plugin.get_plugin_name()
      self.collector_plugins[plugin_name] = plugin
      self.logger.info(f"Registered collector plugin: {plugin_name}")

    def register_output_plugin(self, plugin: BaseOutputPlugin[T]):
      """Register an output plugin."""
      if not plugin.validate_config():
        raise ValueError(f"Invalid configuration for plugin: {plugin.get_plugin_name()}")

      plugin_name = plugin.get_plugin_name()
      self.output_plugins[plugin_name] = plugin
      self.logger.info(f"Registered output plugin: {plugin_name}")

    def add_filter(self, filter_func: Callable[[T], bool]):
      """Add a filter function for data items."""
      self.filters.append(filter_func)

    def add_processor(self, processor_func: Callable[[T], T]):
      """Add a processor function for data items."""
      self.processors.append(processor_func)

    def collect_all_data(self) -> List[T]:
      """Collect data from all enabled collector plugins."""
      all_data = []

      with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        future_to_plugin = {}

        for plugin_name, plugin in self.collector_plugins.items():
          if plugin.is_enabled():
            future = executor.submit(self._collect_from_plugin, plugin)
            future_to_plugin[future] = plugin_name

        for future in as_completed(future_to_plugin):
          plugin_name = future_to_plugin[future]
          try:
            data_items = future.result(timeout=self.collector_plugins[plugin_name].config.timeout)
            all_data.extend(data_items)
            self.logger.info(f"Collected {len(data_items)} items from {plugin_name}")
          except Exception as e:
            self.collector_plugins[plugin_name].handle_error(e)

      return all_data

    def _collect_from_plugin(self, plugin: BaseCollectorPlugin[T]) -> List[T]:
      """Collect data from a single plugin."""
      try:
        plugin.last_run = datetime.now()
        return plugin.collect_data()
      except Exception as e:
        plugin.handle_error(e)
        return []

    def process_data(self, data_items: List[T]) -> List[T]:
      """Process data items through filters and processors."""
      processed_data = []

      for data_item in data_items:
        # Apply filters
        if all(filter_func(data_item) for filter_func in self.filters):
          # Apply processors
          for processor_func in self.processors:
            data_item = processor_func(data_item)

          processed_data.append(data_item)
          self.data_items[data_item.item_id] = data_item

      return processed_data

    def distribute_data(self, data_items: List[T]):
      """Distribute data items to all enabled output plugins."""
      with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        futures = []

        for data_item in data_items:
          for plugin_name, plugin in self.output_plugins.items():
            if plugin.is_enabled() and plugin.should_send(data_item):
              future = executor.submit(self._send_via_plugin, plugin, data_item)
              futures.append((future, plugin_name, data_item.item_id))

        for future, plugin_name, item_id in futures:
          try:
            success = future.result(timeout=self.output_plugins[plugin_name].config.timeout)
            if success:
              self.output_plugins[plugin_name].sent_count += 1
              self.logger.info(f"Sent item {item_id} via {plugin_name}")
          except Exception as e:
            self.output_plugins[plugin_name].handle_error(e)

    def _send_via_plugin(self, plugin: BaseOutputPlugin[T], data_item: T) -> bool:
      """Send data item via a single output plugin."""
      try:
        return plugin.send_data(data_item)
      except Exception as e:
        plugin.handle_error(e)
        return False

    def run_once(self):
      """Run the processing engine once."""
      self.logger.info("Starting processing cycle")

      # Collect data
      collected_data = self.collect_all_data()
      self.logger.info(f"Collected {len(collected_data)} total items")

      # Process data
      processed_data = self.process_data(collected_data)
      self.logger.info(f"Processed {len(processed_data)} items")

      # Distribute data
      if processed_data:
        self.distribute_data(processed_data)

      self.logger.info("Processing cycle complete")

    def get_status(self) -> Dict[str, Any]:
      """Get engine status and statistics."""
      return {
        'running': self.running,
        'total_data_items': len(self.data_items),
        'plugin_directories': self.plugin_dirs,
        'config_directories': self.config_dirs,
        'collector_plugins': {
          name: {
            'status': plugin.status.value,
            'data_type': plugin.get_data_type(),
            'last_run': plugin.last_run.isoformat() if plugin.last_run else None,
            'error_count': plugin.error_count
          }
          for name, plugin in self.collector_plugins.items()
        },
        'output_plugins': {
          name: {
            'status': plugin.status.value,
            'supported_types': plugin.get_supported_data_types(),
            'sent_count': plugin.sent_count,
            'error_count': plugin.error_count
          }
          for name, plugin in self.output_plugins.items()
        }
      }


class AdvisoryProcessingEngine:
  """Main processing engine for advisories with plugin management."""

  def __init__(self, max_workers: int = 5, plugin_dirs: Dict[str, str] = None,
               config_dirs: List[str] = None):
    self.collector_plugins: Dict[str, BaseCollectorPlugin] = {}
    self.output_plugins: Dict[str, BaseOutputPlugin] = {}
    self.advisories: Dict[str, Advisory] = {}
    self.max_workers = max_workers
    self.logger = logging.getLogger("advisory_engine")
    self.running = False
    self.filters: List[Callable[[Advisory], bool]] = []
    self.processors: List[Callable[[Advisory], Advisory]] = []

    # Plugin directories
    self.plugin_dirs = plugin_dirs or {
      'collectors': './plugins/collectors',
      'outputs': './plugins/outputs'
    }

    # Config directories - can include plugin directories and additional paths
    additional_config_dirs = config_dirs or ['./configs']
    self.config_dirs = PluginLoader.find_all_config_directories(
      self.plugin_dirs, additional_config_dirs
    )

    # Load plugins and configurations
    self.plugin_configs = PluginLoader.load_plugin_configs(self.config_dirs)
    self.auto_load_plugins()

  def auto_load_plugins(self):
    """Automatically load all plugins from configured directories."""
    self.logger.info("Auto-loading plugins from directories")

    # Load collector plugins
    collector_classes = PluginLoader.load_plugins_from_directory(
      self.plugin_dirs['collectors'], BaseCollectorPlugin
    )

    for class_name, plugin_class in collector_classes.items():
      try:
        # Get configuration for this plugin
        config_name = class_name.lower().replace('collector', '')
        config = self.plugin_configs.get(config_name, PluginConfig(config_name))

        # Create and register plugin instance
        plugin_instance = plugin_class(config)
        self.register_collector_plugin(plugin_instance)

      except Exception as e:
        self.logger.error(f"Failed to instantiate collector plugin {class_name}: {str(e)}")

    # Load output plugins
    output_classes = PluginLoader.load_plugins_from_directory(
      self.plugin_dirs['outputs'], BaseOutputPlugin
    )

    for class_name, plugin_class in output_classes.items():
      try:
        # Get configuration for this plugin
        config_name = class_name.lower().replace('outputplugin', '').replace('output', '')
        config = self.plugin_configs.get(config_name, PluginConfig(config_name))

        # Create and register plugin instance
        plugin_instance = plugin_class(config)
        self.register_output_plugin(plugin_instance)

      except Exception as e:
        self.logger.error(f"Failed to instantiate output plugin {class_name}: {str(e)}")

  def reload_plugins(self):
    """Reload all plugins from directories."""
    self.logger.info("Reloading all plugins")

    # Clear existing plugins
    self.collector_plugins.clear()
    self.output_plugins.clear()

    # Reload configurations from all config directories
    self.plugin_configs = PluginLoader.load_plugin_configs(self.config_dirs)

    # Auto-load plugins again
    self.auto_load_plugins()

  def load_plugin_from_file(self, plugin_file: str, plugin_type: str):
    """
    Load a specific plugin from a file.

    Args:
        plugin_file: Path to the plugin file
        plugin_type: 'collector' or 'output'
    """
    try:
      plugin_path = Path(plugin_file)

      # Import the module
      spec = importlib.util.spec_from_file_location(
        plugin_path.stem, plugin_path
      )
      module = importlib.util.module_from_spec(spec)
      spec.loader.exec_module(module)

      # Determine base class based on plugin type
      base_class = BaseCollectorPlugin if plugin_type == 'collector' else BaseOutputPlugin

      # Find plugin classes in the module
      for name, obj in inspect.getmembers(module):
        if (inspect.isclass(obj) and
                issubclass(obj, base_class) and
                obj != base_class):

          # Get configuration
          config_name = name.lower().replace('collector', '').replace('outputplugin', '').replace('output', '')
          config = self.plugin_configs.get(config_name, PluginConfig(config_name))

          # Create and register plugin
          plugin_instance = obj(config)

          if plugin_type == 'collector':
            self.register_collector_plugin(plugin_instance)
          else:
            self.register_output_plugin(plugin_instance)

          self.logger.info(f"Loaded {plugin_type} plugin: {name} from {plugin_file}")

    except Exception as e:
      self.logger.error(f"Failed to load plugin from {plugin_file}: {str(e)}")

  def get_available_plugins(self) -> Dict[str, List[str]]:
    """Get list of available plugin files in plugin directories."""
    available = {'collectors': [], 'outputs': []}

    for plugin_type, plugin_dir in self.plugin_dirs.items():
      plugin_path = Path(plugin_dir)
      if plugin_path.exists():
        available[plugin_type] = [
          str(f) for f in plugin_path.glob("*.py")
          if not f.name.startswith("__")
        ]

    return available
    """Register a collector plugin."""
    if not plugin.validate_config():
      raise ValueError(f"Invalid configuration for plugin: {plugin.get_plugin_name()}")

    plugin_name = plugin.get_plugin_name()
    self.collector_plugins[plugin_name] = plugin
    self.logger.info(f"Registered collector plugin: {plugin_name}")

  def register_output_plugin(self, plugin: BaseOutputPlugin):
    """Register an output plugin."""
    if not plugin.validate_config():
      raise ValueError(f"Invalid configuration for plugin: {plugin.get_plugin_name()}")

    plugin_name = plugin.get_plugin_name()
    self.output_plugins[plugin_name] = plugin
    self.logger.info(f"Registered output plugin: {plugin_name}")

  def add_filter(self, filter_func: Callable[[Advisory], bool]):
    """Add a filter function for advisories."""
    self.filters.append(filter_func)

  def add_processor(self, processor_func: Callable[[Advisory], Advisory]):
    """Add a processor function for advisories."""
    self.processors.append(processor_func)

  def collect_all_advisories(self) -> List[Advisory]:
    """Collect advisories from all enabled collector plugins."""
    all_advisories = []

    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
      future_to_plugin = {}

      for plugin_name, plugin in self.collector_plugins.items():
        if plugin.is_enabled():
          future = executor.submit(self._collect_from_plugin, plugin)
          future_to_plugin[future] = plugin_name

      for future in as_completed(future_to_plugin):
        plugin_name = future_to_plugin[future]
        try:
          advisories = future.result(timeout=self.collector_plugins[plugin_name].config.timeout)
          all_advisories.extend(advisories)
          self.logger.info(f"Collected {len(advisories)} advisories from {plugin_name}")
        except Exception as e:
          self.collector_plugins[plugin_name].handle_error(e)

    return all_advisories

  def _collect_from_plugin(self, plugin: BaseCollectorPlugin) -> List[Advisory]:
    """Collect advisories from a single plugin."""
    try:
      plugin.last_run = datetime.now()
      return plugin.collect_advisories()
    except Exception as e:
      plugin.handle_error(e)
      return []

  def process_advisories(self, advisories: List[Advisory]) -> List[Advisory]:
    """Process advisories through filters and processors."""
    processed_advisories = []

    for advisory in advisories:
      # Apply filters
      if all(filter_func(advisory) for filter_func in self.filters):
        # Apply processors
        for processor_func in self.processors:
          advisory = processor_func(advisory)

        processed_advisories.append(advisory)
        self.advisories[advisory.advisory_id] = advisory

    return processed_advisories

  def distribute_advisories(self, advisories: List[Advisory]):
    """Distribute advisories to all enabled output plugins."""
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
      futures = []

      for advisory in advisories:
        for plugin_name, plugin in self.output_plugins.items():
          if plugin.is_enabled() and plugin.should_send(advisory):
            future = executor.submit(self._send_via_plugin, plugin, advisory)
            futures.append((future, plugin_name, advisory.advisory_id))

      for future, plugin_name, advisory_id in futures:
        try:
          success = future.result(timeout=self.output_plugins[plugin_name].config.timeout)
          if success:
            self.output_plugins[plugin_name].sent_count += 1
            self.logger.info(f"Sent advisory {advisory_id} via {plugin_name}")
        except Exception as e:
          self.output_plugins[plugin_name].handle_error(e)

  def _send_via_plugin(self, plugin: BaseOutputPlugin, advisory: Advisory) -> bool:
    """Send advisory via a single output plugin."""
    try:
      return plugin.send_advisory(advisory)
    except Exception as e:
      plugin.handle_error(e)
      return False

  def run_once(self):
    """Run the processing engine once."""
    self.logger.info("Starting advisory processing cycle")

    # Collect advisories
    collected_advisories = self.collect_all_advisories()
    self.logger.info(f"Collected {len(collected_advisories)} total advisories")

    # Process advisories
    processed_advisories = self.process_advisories(collected_advisories)
    self.logger.info(f"Processed {len(processed_advisories)} advisories")

    # Distribute advisories
    if processed_advisories:
      self.distribute_advisories(processed_advisories)

    self.logger.info("Advisory processing cycle complete")

  def get_status(self) -> Dict[str, Any]:
    """Get engine status and statistics."""
    return {
      'running': self.running,
      'total_advisories': len(self.advisories),
      'plugin_directories': self.plugin_dirs,
      'config_directories': self.config_dirs,
      'available_plugins': self.get_available_plugins(),
      'collector_plugins': {
        name: {
          'status': plugin.status.value,
          'last_run': plugin.last_run.isoformat() if plugin.last_run else None,
          'error_count': plugin.error_count
        }
        for name, plugin in self.collector_plugins.items()
      },
      'output_plugins': {
        name: {
          'status': plugin.status.value,
          'sent_count': plugin.sent_count,
          'error_count': plugin.error_count
        }
        for name, plugin in self.output_plugins.items()
      }
    }
