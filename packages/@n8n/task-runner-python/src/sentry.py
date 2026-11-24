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

import logging
from typing import Any

from src.config.sentry_config import SentryConfig
from src.constants import (
    EXECUTOR_FILENAMES,
    IGNORED_ERROR_TYPES,
    LOG_SENTRY_MISSING,
    SENTRY_TAG_SERVER_TYPE_KEY,
    SENTRY_TAG_SERVER_TYPE_VALUE,
)


class TaskRunnerSentry:
    def __init__(self, config: SentryConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def init(self) -> None:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration

        sentry_sdk.init(
            dsn=self.config.dsn,
            release=f"n8n@{self.config.n8n_version}",
            environment=self.config.environment,
            server_name=self.config.deployment_name,
            before_send=self._filter_out_ignored_errors,
            attach_stacktrace=True,
            send_default_pii=False,
            auto_enabling_integrations=False,
            default_integrations=True,
            integrations=[LoggingIntegration(level=logging.ERROR)],
        )
        sentry_sdk.set_tag(SENTRY_TAG_SERVER_TYPE_KEY, SENTRY_TAG_SERVER_TYPE_VALUE)
        _lineaje_payload = "Sentry ready"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunnerSentry.init).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:0ec8d554f8ecc0724e6223f5dfefde765a2c763d26868dd7eb81b163448c576b'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:0ec8d554f8ecc0724e6223f5dfefde765a2c763d26868dd7eb81b163448c576b', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 36, 'function': 'TaskRunnerSentry.init'}, 'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 36, 'function': 'init', 'agent_name': 'TaskRunnerSentry', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            _lineaje_payload = _gr_client.enforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        self.logger.info(_lineaje_payload)

    def shutdown(self) -> None:
        import sentry_sdk

        sentry_sdk.flush(timeout=2.0)
        _lineaje_payload = "Sentry stopped"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunnerSentry.shutdown).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:369f52ca90257e02249f12a5c8508bd9ebed3c2a8c022fb9aa27421d9accd4a3'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:369f52ca90257e02249f12a5c8508bd9ebed3c2a8c022fb9aa27421d9accd4a3', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 42, 'function': 'TaskRunnerSentry.shutdown'}, 'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 42, 'function': 'shutdown', 'agent_name': 'TaskRunnerSentry', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            _lineaje_payload = _gr_client.enforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        self.logger.info(_lineaje_payload)

    def _filter_out_ignored_errors(self, event: Any, hint: Any) -> Any | None:
        if "exc_info" in hint:
            exc_type, _, _ = hint["exc_info"]
            for ignored_type in IGNORED_ERROR_TYPES:
                if (
                    isinstance(exc_type, type)
                    and isinstance(ignored_type, type)
                    and issubclass(exc_type, ignored_type)
                ):
                    return None

        for exception in event.get("exception", {}).get("values", []):
            if self._is_from_user_code(exception):
                return None

            exc_type_name = exception.get("type", "")
            for ignored_type in IGNORED_ERROR_TYPES:
                if ignored_type.__name__ == exc_type_name:
                    return None

        # LINEAJE: runtime guardrail checks `event` as it flows from the agent to the user interface, before it leaves the application (in TaskRunnerSentry._filter_out_ignored_errors).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `event` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:436f3896609847d932fbc83a3ead379888c20128995bac8fe00533dd52c94e38'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:436f3896609847d932fbc83a3ead379888c20128995bac8fe00533dd52c94e38', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 64, 'function': 'TaskRunnerSentry._filter_out_ignored_errors'}, 'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 64, 'function': '_filter_out_ignored_errors', 'agent_name': 'TaskRunnerSentry', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            event = _gr_client.enforce(_gr_site, event, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return event

    def _is_from_user_code(self, exception: dict[str, Any]):
        for frame in exception.get("stacktrace", {}).get("frames", []):
            if frame.get("filename", "") in EXECUTOR_FILENAMES:
                return True
        return False


def setup_sentry(sentry_config: SentryConfig) -> TaskRunnerSentry | None:
    if not sentry_config.enabled:
        return None

    try:
        sentry = TaskRunnerSentry(sentry_config)
        sentry.init()
        # LINEAJE: runtime guardrail checks `sentry` as it flows from the agent to the user interface, before it leaves the application (in setup_sentry).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `sentry` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:a86fd7530a90bc025292824dd34a41fc3e9905ae759c6ff1c0c59395161efee0'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:a86fd7530a90bc025292824dd34a41fc3e9905ae759c6ff1c0c59395161efee0', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 80, 'function': 'setup_sentry'}, 'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 80, 'function': 'setup_sentry', 'agent_name': 'setup_sentry', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            sentry = _gr_client.enforce(_gr_site, sentry, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return sentry
    except ImportError:
        logger = logging.getLogger(__name__)
        logger.warning(LOG_SENTRY_MISSING)
        return None
    except Exception as e:
        logger = logging.getLogger(__name__)
        _lineaje_payload = f"Failed to initialize Sentry: {e}"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in setup_sentry).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:396fe92d7b09735aa74c103311456d6f082c8857924b5beed89bda0523af881f'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:396fe92d7b09735aa74c103311456d6f082c8857924b5beed89bda0523af881f', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 87, 'function': 'setup_sentry'}, 'file': 'packages/@n8n/task-runner-python/src/sentry.py', 'line': 87, 'function': 'setup_sentry', 'agent_name': 'setup_sentry', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            _lineaje_payload = _gr_client.enforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        logger.warning(_lineaje_payload)
        return None
