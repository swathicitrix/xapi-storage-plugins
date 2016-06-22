import logging
import logging.handlers
import sys

LOG_LEVEL = logging.DEBUG
# LOG to local2 which is currently mapped to /var/log/SMlog for SM
LOG_SYSLOG_FACILITY = logging.handlers.SysLogHandler.LOG_LOCAL2
LOG_TO_STDERR = False

class Log(object):

    instance = None

    def __init__(self):
        self.configure_logging()

    def configure_logging(self):
        self._LOGGER = logging.getLogger()
        self._LOGGER.setLevel(LOG_LEVEL)

        formatter = logging.Formatter(
            'SMAPIv3: [%(process)d] - %(levelname)s - %(message)s')

        handlers = []

        # Log to syslog
        handlers.append(logging.handlers.SysLogHandler(
                address='/dev/log',
                facility=LOG_SYSLOG_FACILITY))

        if LOG_TO_STDERR:
            # Write to stderr
            handlers.append(logging.StreamHandler(sys.stderr))

        # Configure and add handlers
        for handler in handlers:
            handler.setLevel(LOG_LEVEL)
            handler.setFormatter(formatter)
            self._LOGGER.addHandler(handler)

    @staticmethod
    def get_instance():
        if (Log.instance == None):
            Log.instance = Log()
        return Log.instance

    @staticmethod
    def debug(message, *args, **kwargs):
        Log.get_instance()._LOGGER.debug(message, *args, **kwargs)


    @staticmethod
    def info(message, *args, **kwargs):
        Log.get_instance()._LOGGER.info(message, *args, **kwargs)


    @staticmethod
    def error(message, *args, **kwargs):
        Log.get_instance()._LOGGER.error(message, *args, **kwargs)


    def log_call_argv():
        info("called as: %s" % (sys.argv))

