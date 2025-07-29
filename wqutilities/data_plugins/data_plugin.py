from wqutilities import logging

from yapsy.IPlugin import IPlugin
from multiprocessing import Process

logger = logging.getLogger(__name__)


class DataCollectorPlugin(IPlugin, Process):
    def __init__(self):
        Process.__init__(self)
        IPlugin.__init__(self)
        self._logger = logger
        self._plugin_details = None
        self._logging_client_cfg = None
        self._input_queue = None
        self._output_queue = None

    @property
    def input_queue(self):
      return self._input_queue
    @property
    def output_queue(self):
      return self._output_queue

    def initialize_plugin(self, **kwargs):
        self._plugin_details = kwargs['details']
        base_logfile_name = kwargs['logfile_name']
        self.logging_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'f': {
                    'format': "%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s",
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                }
            },
            'handlers': {
                'stream': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'f',
                    'level': logging.DEBUG
                },
                'file_handler': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': base_logfile_name,
                    'formatter': 'f',
                    'level': logging.DEBUG
                }
            },
            'root': {
                'handlers': ['file_handler'],
                'level': logging.NOTSET,
                'propagate': False
            }
        }

    def run(self):
        raise Exception("Must be implemented by child.")

    def finalize(self):
        self.logger.info("Closing loggers.")
