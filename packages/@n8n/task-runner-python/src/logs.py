# Copyright (c) Lineaje, Inc. All rights reserved.
# Lineaje UnifAI guardrail  version=2.0.0-alpha
def _lineaje_gr_load(_f):
    """Lineaje-added: load gr_stub_client.py beside _f or in one of its 7 parent folders. Missing or broken:
    a pass-through client and ONE warning per process (GR_REQUIRE_CLIENT=1: ImportError instead)."""
    import sys, os, logging, importlib.util as u, pathlib
    n = "_lineaje_gr_stub_client"
    if n in sys.modules: return sys.modules[n]
    h = pathlib.Path(_f or ".").resolve().parent
    d = [h, *h.parents][:8]
    p = next((x / "gr_stub_client.py" for x in d if (x / "gr_stub_client.py").is_file()), None)
    try:
        s = u.spec_from_file_location(n, p) if p else None
        if s is None: raise FileNotFoundError("gr_stub_client.py")
        m = sys.modules[n] = u.module_from_spec(s); s.loader.exec_module(m); return m
    except Exception as e:
        sys.modules.pop(n, None)
        w = ("gr_stub_client.py not found for %s (searched: %s)" % (_f or "<no __file__>", ", ".join(map(str, d)))
             if p is None else "%s failed to load (%s%s)" % (p, type(e).__name__,
                                                          " line %s" % e.lineno if isinstance(e, SyntaxError) else ""))
        w = ("Lineaje guardrail NOT active: " + w + ". Calls pass through unchecked; set GR_REQUIRE_CLIENT=1 to fail "
             "instead. Each deploy unit needs gr_stub_client.py beside its code and GR_SERVICE_URL plus credentials as "
             "environment variables.")
        if os.environ.get("GR_REQUIRE_CLIENT", "").strip() == "1": raise ImportError(w) from (None if p is None else e)
        z = sys.__dict__.setdefault("_lineaje_gr_null", __import__('types').SimpleNamespace(STUB_API=999999, GuardrailUnavailableError=type('GuardrailUnavailableError', (PermissionError,), {}), SiteDescriptor=lambda *a, **k: None, new_interaction_id=lambda *a, **k: None, enforce=lambda _s, _p, *a, **k: _p, enforce_request=lambda _s, _p, *a, **k: _p, enforce_reply=lambda _s, _p, *a, **k: _p, _lineaje_seen=set()))
        k = "missing" if p is None else (str(p), type(e).__name__)
        if k not in z._lineaje_seen:
            z._lineaje_seen.add(k); g = logging.getLogger("lineaje.guardrail"); g.log(30 if p is None else 40, w)
            if g.hasHandlers(): sys.stderr.write(w + "\n")
        return z
__import__("sys").__dict__.setdefault("_lineaje_gr_load_v1", _lineaje_gr_load)
del _lineaje_gr_load
def _lineaje_load_gr_client():
    """Lineaje-added: this file's gr_stub_client.py (the loader _lineaje_gr_load registered)."""
    return __import__("sys").__dict__["_lineaje_gr_load_v1"](globals().get("__file__"))
if __import__("os").environ.get("GR_REQUIRE_CLIENT", "").strip() == "1": __import__("sys").__dict__["_lineaje_gr_load_v1"](globals().get("__file__"))

import sys
import logging
import os
from src.constants import LOG_FORMAT, LOG_TIMESTAMP_FORMAT, ENV_LAUNCHER_LOG_LEVEL

COLORS = {
    "DEBUG": "\033[34m",  # blue
    "INFO": "\033[32m",  # green
    "WARNING": "\033[33m",  # yellow
    "ERROR": "\033[31m",  # red
    "CRITICAL": "\033[31m",  # red
}

RESET = "\033[0m"


class ColorFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.use_colors = os.getenv("NO_COLOR") is None

        # When started by launcher, log level and timestamp are handled by launcher.
        self.short_form = not sys.stdout.isatty()

    def format(self, record):
        if self.short_form:
            return record.getMessage()

        formatted = super().format(record)

        if not self.use_colors:
            # LINEAJE: runtime guardrail checks `formatted` as it flows from the agent to the user interface, before it leaves the application (in ColorFormatter.format).
            #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `formatted` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:726d96703b8d6d200c5602529f984a94ed12474d80d6d3310e7790a220b3dc50'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:726d96703b8d6d200c5602529f984a94ed12474d80d6d3310e7790a220b3dc50', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/logs.py', 'line': 33, 'function': 'ColorFormatter.format'}, 'file': 'packages/@n8n/task-runner-python/src/logs.py', 'line': 33, 'function': 'format', 'agent_name': 'ColorFormatter', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                formatted = _gr_client.enforce(_gr_site, formatted, content_type='text/plain')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            return formatted

        parts = formatted.split("\t")

        if len(parts) >= 3:
            timestamp = parts[0]
            level = parts[1]
            message = " ".join(parts[2:])

            level_color = COLORS.get(record.levelname, "")
            if level_color:
                level = level_color + level + RESET
                message = level_color + message + RESET

            formatted = f"{timestamp}  {level}  {message}"

        # LINEAJE: runtime guardrail checks `formatted` as it flows from the agent to the user interface, before it leaves the application (in ColorFormatter.format).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `formatted` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:96707be7b41ab2ca5c12a3718bbec17edc8e1e48da59161fcda0d0afb9977595'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:96707be7b41ab2ca5c12a3718bbec17edc8e1e48da59161fcda0d0afb9977595', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/logs.py', 'line': 49, 'function': 'ColorFormatter.format'}, 'file': 'packages/@n8n/task-runner-python/src/logs.py', 'line': 49, 'function': 'format', 'agent_name': 'ColorFormatter', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            formatted = _gr_client.enforce(_gr_site, formatted, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return formatted


def setup_logging():
    logger = logging.getLogger()

    log_level_str = os.getenv(ENV_LAUNCHER_LOG_LEVEL, "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(ColorFormatter(LOG_FORMAT, LOG_TIMESTAMP_FORMAT))
    logger.addHandler(stream_handler)

    # Hardcoded to INFO as websocket logs are too verbose
    logging.getLogger("websockets.client").setLevel(logging.INFO)
    logging.getLogger("websockets.server").setLevel(logging.INFO)
    logging.getLogger("websockets").setLevel(logging.INFO)
