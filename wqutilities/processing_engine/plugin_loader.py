from typing import Dict, List, Type
import json
import logging
import sys
import importlib.util
import inspect
from pathlib import Path
import configparser

from .plugin_base import PluginConfig

class PluginLoader:
  """Utility class for loading plugins from directories."""

  @staticmethod
  def load_plugins_from_directory(plugin_dir: str, base_class: Type) -> Dict[str, Type]:
    """
    Load all plugins from a directory that inherit from the specified base class.

    Args:
        plugin_dir: Directory path containing plugin files
        base_class: Base class that plugins should inherit from

    Returns:
        Dictionary of plugin name to plugin class
    """
    plugins = {}
    plugin_path = Path(plugin_dir)

    if not plugin_path.exists():
      logging.warning(f"Plugin directory does not exist: {plugin_dir}")
      return plugins

    # Add plugin directory to Python path
    if str(plugin_path.absolute()) not in sys.path:
      sys.path.insert(0, str(plugin_path.absolute()))

    # Load all Python files in the directory
    for plugin_file in plugin_path.glob("*.py"):
      if plugin_file.name.startswith("__"):
        continue

      try:
        # Import the module
        spec = importlib.util.spec_from_file_location(
          plugin_file.stem, plugin_file
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Find all classes that inherit from base_class
        for name, obj in inspect.getmembers(module):
          if (inspect.isclass(obj) and
                  issubclass(obj, base_class) and
                  obj != base_class):
            plugins[name] = obj
            logging.info(f"Loaded plugin: {name} from {plugin_file.name}")

      except Exception as e:
        logging.error(f"Failed to load plugin from {plugin_file}: {str(e)}")
        logging.exception(e)

    return plugins

  @staticmethod
  def load_plugin_configs(config_dirs: List[str]) -> Dict[str, PluginConfig]:
    """
    Load plugin configurations from JSON files in multiple directories.

    Args:
        config_dirs: List of directories containing JSON configuration files

    Returns:
        Dictionary of plugin name to PluginConfig
    """
    configs = {}

    for config_dir in config_dirs:
      config_path = Path(config_dir)

      if not config_path.exists():
        logging.warning(f"Config directory does not exist: {config_dir}")
        continue

      for config_file in config_path.glob("*.json"):
        try:
          with open(config_file, 'r') as f:
            config_data = json.load(f)

          plugin_name = config_file.stem
          configs[plugin_name] = PluginConfig(
            name=plugin_name,
            enabled=config_data.get('enabled', True),
            config=config_data.get('config', {}),
            retry_count=config_data.get('retry_count', 3),
            timeout=config_data.get('timeout', 30)
          )
          logging.info(f"Loaded config for plugin: {plugin_name} from {config_dir}")

        except Exception as e:
          logging.error(f"Failed to load config from {config_file}: {str(e)}")

      for config_file in config_path.glob("*.ini"):
        try:
          config_file = configparser.SafeConfigParser(
            defaults={
              'enabled': True,
              'config': {},
              'retry_count': 3,
              'timeout': 30
            }
          )
          config_file.read(config_file)

          plugin_name = config_file.stem
          configs[plugin_name] = PluginConfig(
            name=plugin_name,
            enabled=config_file.get('default', 'enabled'),
            config=config_file.get('default', 'config'),
            retry_count=config_file.get('default', 'retry_count'),
            timeout=config_file.get('default', 'timeout')
          )
          logging.info(f"Loaded config for plugin: {plugin_name} from {config_dir}")

        except Exception as e:
          logging.error(f"Failed to load config from {config_file}: {str(e)}")

    return configs

  @staticmethod
  def find_all_config_directories(plugin_dirs: Dict[str, str], additional_config_dirs: List[str] = None) -> List[str]:
    """
    Find all directories that should be searched for config files.

    Args:
        plugin_dirs: Dictionary of plugin directories
        additional_config_dirs: Additional config directories to search

    Returns:
        List of all config directories to search
    """
    config_dirs = []

    # Add plugin directories (configs can be co-located with plugins)
    for plugin_dir in plugin_dirs.values():
      config_dirs.append(plugin_dir)

    # Add additional config directories
    if additional_config_dirs:
      config_dirs.extend(additional_config_dirs)

    return config_dirs
