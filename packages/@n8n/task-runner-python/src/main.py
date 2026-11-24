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

import asyncio
import logging
import sys
import platform

from src.constants import ERROR_WINDOWS_NOT_SUPPORTED
from src.config.health_check_config import HealthCheckConfig
from src.config.sentry_config import SentryConfig
from src.config.task_runner_config import TaskRunnerConfig
from src.errors import ConfigurationError
from src.logs import setup_logging
from src.task_runner import TaskRunner
from src.shutdown import Shutdown


async def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    sentry = None
    sentry_config = SentryConfig.from_env()

    if sentry_config.enabled:
        from src.sentry import setup_sentry

        sentry = setup_sentry(sentry_config)

    try:
        health_check_config = HealthCheckConfig.from_env()
    except ConfigurationError as e:
        _lineaje_payload = f"Invalid health check configuration: {e}"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in main).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:a3f6182a14f7c6a8cc0fab5f5b912e221a76656103b38c1190a5c824315b4357'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:a3f6182a14f7c6a8cc0fab5f5b912e221a76656103b38c1190a5c824315b4357', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 31, 'function': 'main'}, 'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 31, 'function': 'main', 'agent_name': 'main', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        logger.error(_lineaje_payload)
        sys.exit(1)

    health_check_server: "HealthCheckServer | None" = None
    if health_check_config.enabled:
        from src.health_check_server import HealthCheckServer

        health_check_server = HealthCheckServer()
        try:
            await health_check_server.start(health_check_config)
        except OSError as e:
            _lineaje_payload = f"Failed to start health check server: {e}"
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in main).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:5d29f784086f3f65ad67d0d70d38935670da84660b98f183b9e49ea33942f3b6'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:5d29f784086f3f65ad67d0d70d38935670da84660b98f183b9e49ea33942f3b6', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 42, 'function': 'main'}, 'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 42, 'function': 'main', 'agent_name': 'main', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            logger.error(_lineaje_payload)
            sys.exit(1)

    try:
        task_runner_config = TaskRunnerConfig.from_env()
    except ConfigurationError as e:
        _lineaje_payload = str(e)
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in main).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:fe23fa6be4a4249d8fe3342128fafa4d5c9d1ab24b3988b0ef3257f651b94bf4'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:fe23fa6be4a4249d8fe3342128fafa4d5c9d1ab24b3988b0ef3257f651b94bf4', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 48, 'function': 'main'}, 'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 48, 'function': 'main', 'agent_name': 'main', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        logger.error(_lineaje_payload)
        sys.exit(1)

    task_runner = TaskRunner(task_runner_config)
    _lineaje_payload = "Starting runner..."
    # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in main).
    #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
    #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
    #   Do not remove without review. site_id='site:sha256:e6689507813382584fe989953ec1cafc57c302467aeab7a33bfe70aef97eea34'
    _gr_client = _lineaje_load_gr_client()
    _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:e6689507813382584fe989953ec1cafc57c302467aeab7a33bfe70aef97eea34', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 52, 'function': 'main'}, 'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 52, 'function': 'main', 'agent_name': 'main', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
    try:
        if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
            __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
            raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
        _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
    except _gr_client.GuardrailUnavailableError:
        pass
    except PermissionError:
        pass
    logger.info(_lineaje_payload)

    shutdown = Shutdown(task_runner, health_check_server, sentry)
    task_runner.on_idle_timeout = shutdown.start_auto_shutdown

    try:
        await task_runner.start()
    except Exception:
        _lineaje_payload = "Unexpected error"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in main).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:e7eca059c170ffd87ead7b6d280bcb02951de14a0c63f240e5538d9f3663e8c9'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:e7eca059c170ffd87ead7b6d280bcb02951de14a0c63f240e5538d9f3663e8c9', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 60, 'function': 'main'}, 'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 60, 'function': 'main', 'agent_name': 'main', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        logger.error(_lineaje_payload, exc_info=True)
        await shutdown.start_shutdown()

    exit_code = await shutdown.wait_for_shutdown()
    sys.exit(exit_code)


if __name__ == "__main__":
    if platform.system() == "Windows":
        try:
            _gr_client_m52dd7aef = __import__("sys").__dict__["_lineaje_gr_load_v1"](globals().get("__file__"))
        except KeyError:
            _gr_client_m52dd7aef = __import__('types').SimpleNamespace(STUB_API=999999, GuardrailUnavailableError=type('GuardrailUnavailableError', (PermissionError,), {}), SiteDescriptor=lambda *a, **k: None, new_interaction_id=lambda *a, **k: None, enforce=lambda _s, _p, *a, **k: _p, enforce_request=lambda _s, _p, *a, **k: _p, enforce_reply=lambda _s, _p, *a, **k: _p, _lineaje_seen=set())
        # LINEAJE: runtime guardrail checks `ERROR_WINDOWS_NOT_SUPPORTED` as it flows from the agent to the application log, before it is written to the log (in main).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `ERROR_WINDOWS_NOT_SUPPORTED` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:0a6dc5b2f727f28fe23507a11bfdc1285d25eb790230c8156a8af5fe7cb1fc4b'
        _gr_site_m52dd7aef = _gr_client_m52dd7aef.SiteDescriptor(site_id='site:sha256:0a6dc5b2f727f28fe23507a11bfdc1285d25eb790230c8156a8af5fe7cb1fc4b', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 69, 'function': ''}, 'file': 'packages/@n8n/task-runner-python/src/main.py', 'line': 69, 'function': 'main', 'agent_name': 'main', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            ERROR_WINDOWS_NOT_SUPPORTED = _gr_client_m52dd7aef.enforce(_gr_site_m52dd7aef, ERROR_WINDOWS_NOT_SUPPORTED, content_type='application/json')
        except _gr_client_m52dd7aef.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        __import__('builtins').list(map(globals().pop, ('_gr_client_m52dd7aef', '_gr_site_m52dd7aef'), (None,) * 2))
        print(ERROR_WINDOWS_NOT_SUPPORTED, file=sys.stderr)
        sys.exit(1)

    asyncio.run(main())
