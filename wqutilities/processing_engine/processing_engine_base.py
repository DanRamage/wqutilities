from datetime import datetime
from typing import Dict, Any, List, Callable, Generic
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from .plugin_loader import PluginLoader
from .plugin_base import BaseCollectorPlugin, BaseOutputPlugin, T, PluginConfig

logger = logging.getLogger(__name__)

class GenericProcessingEngine(Generic[T]):
    """Generic processing engine for any data type with plugin management."""

    def __init__(self, max_workers: int = 5,
                 plugin_dirs: Dict[str, str] = None,
                 plugins_enabled: Dict[str, bool] = None,
                 config_dirs: List[str] = None,
                 process_data_batch: bool = True,
                 distribute_data_batch: bool = True):
      self.collector_plugins: Dict[str, BaseCollectorPlugin[T]] = {}
      self.output_plugins: Dict[str, BaseOutputPlugin[T]] = {}
      self.data_items: Dict[str, T] = {}
      self.max_workers = max_workers
      self.logger = logger
      self.running = False
      self.filters: List[Callable[[T], bool]] = []
      self.processors: List[Callable[[T], T]] = []
      self.process_data_batch = process_data_batch
      self.distribute_data_batch = distribute_data_batch

      # Plugin directories
      self.plugin_dirs = plugin_dirs or {
        'collectors': './plugins/collectors',
        'outputs': './plugins/outputs'
      }
      #Flags that enabled or disabled all the collector or output plugins.
      self.plugins_enabled = plugins_enabled or {
        'collectors': True,
        'outputs': True
      }

      # Config directories - can include plugin directories and additional paths
      '''
      additional_config_dirs = config_dirs or []
      self.config_dirs = PluginLoader.find_all_config_directories(
        self.plugin_dirs, additional_config_dirs
      )
      '''
      self.auto_load_plugins()

    def auto_load_plugins(self):
      """Automatically load all plugins from configured directories."""
      self.logger.info("Auto-loading plugins from directories")

      collector_plugins = PluginLoader(self.plugin_dirs['collectors'],
                                          [],
                                          BaseCollectorPlugin)
      # Load plugins and configurations
      self.plugin_configs = collector_plugins.load_plugin_configs()

      # Load collector plugins
      collector_classes = collector_plugins.discover_plugins()

      for plugin_class in collector_classes:
        try:
          class_name = plugin_class.__name__
          config = self.plugin_configs.get(class_name, PluginConfig(class_name))

          # Create and register plugin instance
          plugin_instance = plugin_class(config)
          self.register_collector_plugin(plugin_instance)

        except Exception as e:
          self.logger.error(f"Failed to instantiate collector plugin {class_name}: {str(e)}")
          self.logger.exception(e)

      output_plugin_loader = PluginLoader(self.plugin_dirs['outputs'],
                                          [],
                                          BaseCollectorPlugin)

      # Load output plugins
      output_classes = output_plugin_loader.discover_plugins()

      for plugin_class in output_classes:
        try:
          class_name = plugin_class.__name__
          config = self.plugin_configs.get(class_name, PluginConfig(class_name))

          # Create and register plugin instance
          plugin_instance = plugin_class(config)
          self.register_output_plugin(plugin_instance)

        except Exception as e:
          self.logger.error(f"Failed to instantiate output plugin {class_name}: {str(e)}")
          self.logger.exception(e)

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
            data_items = future.result(timeout=self.collector_plugins[plugin_name].plugin_config.timeout)
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
    def batch_process_data(self, data_items: List[T]) -> List[T]:
      """Process data items through filters and processors."""
      processed_data = []

      # Apply filters
      if all(filter_func(data_items) for filter_func in self.filters):
        # Apply processors
        for processor_func in self.processors:
          data_items = processor_func(data_items)

        processed_data = data_items
        #processed_data.extend(data_items)
        for data_item in self.data_items:
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

    def batch_distribute_data(self, data_items: List[T]):
      """Distribute data items to all enabled output plugins."""
      with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        futures = []

        #for data_item in data_items:
        for plugin_name, plugin in self.output_plugins.items():
          if plugin.is_enabled() and plugin.should_send(data_items):
            future = executor.submit(self._send_via_plugin, plugin, data_items)
            futures.append((future, plugin_name, data_items.item_id))

        for future, plugin_name, item_id in futures:
          try:
            success = future.result(timeout=self.output_plugins[plugin_name].plugin_config.timeout)
            if success:
              self.output_plugins[plugin_name].sent_count += len(data_items)
              #self.output_plugins[plugin_name].sent_count += 1
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
      if self.process_data_batch:
        processed_data = self.batch_process_data(collected_data)
      else:
        processed_data = self.process_data(collected_data)
      self.logger.info(f"Processed {len(processed_data)} items")

      # Distribute data
      if processed_data:
        if self.distribute_data_batch:
          self.batch_distribute_data(processed_data)
        else:
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

