import sys

import logging.config
from yapsy.PluginManager import PluginManager
from pytz import timezone
import time
from ..data_plugins.data_plugin import DataCollectorPlugin

logger = logging.getLogger(__name__)

class WQPredictionEngine:
  def __init__(self):
    self.logger = logger

  def initialize_engine(self, **kwargs):
    raise "Must be implemented by child class"

  def build_test_objects(self, **kwargs):
    raise "Must be implemented by child class"

  def prepare_data(self, **kargs):
    raise "Must be implemented by child class"

  def run_wq_models(self, **kwargs):
    raise "Must be implemented by child class"

  def collect_data(self, **kwargs):
    self.logger.info("Begin collect_data")

    simplePluginManager = PluginManager()
    logging.getLogger('yapsy').setLevel(logging.DEBUG)
    simplePluginManager.setCategoriesFilter({
       "DataCollector": DataCollectorPlugin
       })

    # Tell it the default place(s) where to find plugins
    self.logger.debug("Plugin directories: %s" % (kwargs['data_collector_plugin_directories']))
    simplePluginManager.setPluginPlaces(kwargs['data_collector_plugin_directories'])

    simplePluginManager.collectPlugins()

    plugin_cnt = 0
    plugin_start_time = time.time()
    for plugin in simplePluginManager.getAllPlugins():
      self.logger.info("Starting plugin: %s" % (plugin.name))
      if plugin.plugin_object.initialize_plugin(details=plugin.details):
        plugin.plugin_object.start()
      else:
        self.logger.error("Failed to initialize plugin: %s" % (plugin.name))
      plugin_cnt += 1

    #Wait for the plugings to finish up.
    self.logger.info("Waiting for %d plugins to complete." % (plugin_cnt))
    for plugin in simplePluginManager.getAllPlugins():
      plugin.plugin_object.join()
      plugin.plugin_object.finalize()

    self.logger.info("%d Plugins completed in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))


  def output_results(self, **kwargs):

    self.logger.info("Begin run_output_plugins")

    simplePluginManager = PluginManager()
    logging.getLogger('yapsy').setLevel(logging.DEBUG)
    simplePluginManager.setCategoriesFilter({
       "OutputResults": output_plugin
       })

    # Tell it the default place(s) where to find plugins
    self.logger.debug("Plugin directories: %s" % (kwargs['output_plugin_directories']))
    simplePluginManager.setPluginPlaces(kwargs['output_plugin_directories'])

    simplePluginManager.collectPlugins()

    plugin_cnt = 0
    plugin_start_time = time.time()
    for plugin in simplePluginManager.getAllPlugins():
      self.logger.info("Starting plugin: %s" % (plugin.name))
      if plugin.plugin_object.initialize_plugin(details=plugin.details):
        plugin.plugin_object.emit(prediction_date=kwargs['prediction_date'].astimezone(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S"),
                                  execution_date=kwargs['prediction_run_date'].strftime("%Y-%m-%d %H:%M:%S"),
                                  ensemble_tests=kwargs['site_model_ensemble'])
        plugin_cnt += 1
      else:
        self.logger.error("Failed to initialize plugin: %s" % (plugin.details))
    self.logger.debug("%d output plugins run in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))
    self.logger.info("Finished collect_data")
