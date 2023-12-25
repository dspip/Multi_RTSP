import logging
import re
import typing
from .xterm import XTERM_COLORS as XT

PIPELINE = logging.INFO + 1
BIT = logging.INFO + 2


class CustomFormatter(logging.Formatter):

    def __init__(self, order=[], version=None):
        def wrap_color(str, color):
            return f"{color}{str}{XT.RESET}"

        _time = "%(asctime)s"
        _order = f"""{"/".join((f"{step}" for step in order))}""" if len(order) > 0 else None
        _level = "%(levelname)s"
        _message = "%(message)s"
        _location = wrap_color("%(filename)s:%(lineno)d", XT.DIM)

        def _common_format(_time, _order, _level, _message, _location):
            c = {}
            c["["] = wrap_color("[", XT.DIM)
            c["]"] = wrap_color("]", XT.DIM)
            c["("] = wrap_color("(", XT.DIM)
            c[")"] = wrap_color(")", XT.DIM)
            __order = f"{c['[']}{_order}{c[']']}" if _order and len(_order) > 0 else ""
            return f"{c['[']}{_time}{c[']']}{__order}{c['[']}{_level}{c[']']} {_message} {c['(']}{_location}{c[')']}"

        def _debug_format(_time, _order, _level, _message, _location):
            _level = wrap_color(_level, XT.Green)
            _message = wrap_color(_message, f"{XT.Green}{XT.DIM}")
            return _common_format(_time, _order, _level, _message, _location)

        def _info_format(_time, _order, _level, _message, _location):
            return _common_format(_time, _order, _level, _message, _location)

        def _warning_format(_time, _order, _level, _message, _location):
            _level = wrap_color(_level, XT.Yellow)
            _message = wrap_color(_message, f"{XT.Yellow}{XT.DIM}")
            return _common_format(_time, _order, _level, _message, _location)

        def _error_format(_time, _order, _level, _message, _location):
            _level = wrap_color(_level, XT.Red)
            _message = wrap_color(_message, f"{XT.Red}{XT.DIM}")
            return _common_format(_time, _order, _level, _message, _location)

        def _critical_format(_time, _order, _level, _message, _location):
            _level = wrap_color(_level, XT.Red)
            _message = wrap_color(_message, f"{XT.Red}{XT.DIM}")
            return _common_format(_time, _order, _level, _message, _location)

        def _pipeline_format(_time, _order, _level, _message, _location):
            _level = "PIPELINE"
            _level = wrap_color(_level, XT.Green)
            _message = wrap_color(_message, f"{XT.Green}{XT.DIM}")
            return _common_format(_time, _order, _level, _message, _location)

        def _bit_format(_time, _order, _level, _message, _location):
            _level = "BIT"
            _level = wrap_color(_level, XT.Magenta1)
            _message = wrap_color(_message, f"{XT.Magenta1}{XT.DIM}")
            return _common_format(_time, _order, _level, _message, _location)

        # datefmt = '%Y-%m-%d %H:%M:%S %s,%03d'
        # datefmt = '%Y-%m-%d %H:%M:%S %03d'
        datefmt = None

        self.formatters = {}
        self.formatters[logging.DEBUG] = logging.Formatter(_debug_format(_time, _order, _level, _message, _location), datefmt=datefmt)
        self.formatters[logging.INFO] = logging.Formatter(_info_format(_time, _order, _level, _message, _location), datefmt=datefmt)
        self.formatters[logging.WARNING] = logging.Formatter(_warning_format(_time, _order, _level, _message, _location), datefmt=datefmt)
        self.formatters[logging.ERROR] = logging.Formatter(_error_format(_time, _order, _level, _message, _location), datefmt=datefmt)
        self.formatters[logging.CRITICAL] = logging.Formatter(_critical_format(_time, _order, _level, _message, _location), datefmt=datefmt)
        self.formatters[PIPELINE] = logging.Formatter(_pipeline_format(_time, _order, _level, _message, _location), datefmt=datefmt)
        self.formatters[BIT] = logging.Formatter(_bit_format(_time, _order, _level, _message, _location), datefmt=datefmt)

    def format(self, record):
        identifier = record.levelno
        formatter = self.formatters.get(identifier, None)
        return formatter.format(record)


class Logger(logging.Logger):
    sub: "typing.Callable[[str], Logger]"
    pipeline: "typing.Callable[[typing.Any], None]"
    bit: "typing.Callable[[typing.Any], None]"
    _order: "list[str]"


def create_logger(name, order=[], version=None) -> Logger:
    if name != None:
        order = order[:] + [name]
    # print(f"order={order}")

    def sub(_name):
        # print(f"sub({_name}), order={order}")
        return create_logger(_name, order, version=version)

    def pipeline(msg, *args, **kwargs):
        kwargs["stacklevel"] = kwargs.get("stacklevel", 1) + 1
        return logger.log(PIPELINE, msg, *args, **kwargs)

    def bit(msg, *args, **kwargs):
        kwargs["stacklevel"] = kwargs.get("stacklevel", 1) + 1
        return logger.log(BIT, msg, *args, **kwargs)

    logger: "Logger" = logging.getLogger("-".join(order))
    logger.sub = sub
    logger.pipeline = pipeline
    logger.bit = bit
    logger._order = order

    # for handler in logger.handlers:
    #     logger.removeHandler(handler)
    # logger.handlers.clear()
    logger.propagate = False
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setFormatter(CustomFormatter(order, version))
        logger.addHandler(ch)
    return logger


def load_package_json():
    import os
    import json
    package_path = "./package.json"
    package = None
    if os.path.isfile(package_path):
        with open(package_path, "r") as f:
            package = json.load(f)
    return package


logger = None
version = None
name = None
try:
    package = load_package_json()
    if package:
        version = package.get("version", None)
        name = package.get("name", None)
except:
    pass

logger = create_logger(name, version=version)
