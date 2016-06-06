import logging
import logging.handlers
import sys


LOG_LEVEL = logging.DEBUG
# LOG to local2 which is currently mapped to /var/log/SMlog for SM
LOG_SYSLOG_FACILITY = logging.handlers.SysLogHandler.LOG_LOCAL2
LOG_TO_STDERR = False


def configure_logging():
    _LOGGER.setLevel(LOG_LEVEL)

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
        _LOGGER.addHandler(handler)


def debug(message, *args, **kwargs):
    _LOGGER.debug(message, *args, **kwargs)


def info(message, *args, **kwargs):
    _LOGGER.info(message, *args, **kwargs)


def error(message, *args, **kwargs):
    _LOGGER.error(message, *args, **kwargs)


def log_call_argv():
    info("called as: %s" % (sys.argv))

_LOGGER = logging.getLogger()
configure_logging()

