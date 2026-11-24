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
import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.task_runner import TaskRunner
    from src.health_check_server import HealthCheckServer
    from src.sentry import TaskRunnerSentry


class Shutdown:
    """Responsible for managing the shutdown routine of the task runner."""

    def __init__(
        self,
        task_runner: "TaskRunner",
        health_check_server: "HealthCheckServer | None" = None,
        sentry: "TaskRunnerSentry | None" = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.is_shutting_down = False
        self.shutdown_complete = asyncio.Event()
        self.exit_code = 0

        self.task_runner = task_runner
        self.health_check_server = health_check_server
        self.sentry = sentry

        self._register_handler(signal.SIGINT)
        self._register_handler(signal.SIGTERM)

    async def start_shutdown(self, custom_timeout: int | None = None):
        if self.is_shutting_down:
            return

        self.is_shutting_down = True

        timeout = (
            custom_timeout
            if custom_timeout is not None
            else self.task_runner.config.graceful_shutdown_timeout
        )

        try:
            await asyncio.wait_for(self._perform_shutdown(), timeout=timeout)
            self.exit_code = 0
        except asyncio.TimeoutError:
            _lineaje_payload = f"Shutdown timed out after {timeout}s, forcing exit..."
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in Shutdown.start_shutdown).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:4987d8eed6a033123003466747af6bc736a27e70d488ba05c54a348feb73eb4e'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:4987d8eed6a033123003466747af6bc736a27e70d488ba05c54a348feb73eb4e', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 49, 'function': 'Shutdown.start_shutdown'}, 'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 49, 'function': 'start_shutdown', 'agent_name': 'Shutdown', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            self.logger.warning(_lineaje_payload)
            self.exit_code = 1
        except Exception as e:
            _lineaje_payload = f"Error during shutdown: {e}"
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in Shutdown.start_shutdown).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:d31ff2507971dd0991a9f50574b9123333bb4982ebb60a2acaa198fa73e01e97'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:d31ff2507971dd0991a9f50574b9123333bb4982ebb60a2acaa198fa73e01e97', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 52, 'function': 'Shutdown.start_shutdown'}, 'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 52, 'function': 'start_shutdown', 'agent_name': 'Shutdown', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            self.logger.error(_lineaje_payload, exc_info=True)
            self.exit_code = 1
        finally:
            self.shutdown_complete.set()

    async def wait_for_shutdown(self) -> int:
        await self.shutdown_complete.wait()
        return self.exit_code

    def _register_handler(self, sig: signal.Signals):
        async def handler():
            _lineaje_payload = f"Received {sig.name} signal, starting shutdown..."
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in Shutdown.handler).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:9d83e04345714ee814a8469da152770131cd76431edf70a2a12af79a3a1ad72b'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:9d83e04345714ee814a8469da152770131cd76431edf70a2a12af79a3a1ad72b', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 63, 'function': 'Shutdown._register_handler.handler'}, 'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 63, 'function': 'handler', 'agent_name': 'Shutdown', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            self.logger.info(_lineaje_payload)
            await self.start_shutdown()

        try:
            asyncio.get_running_loop().add_signal_handler(
                sig, lambda: asyncio.create_task(handler())
            )
        except NotImplementedError:
            _lineaje_payload = f"Signal handler for {sig.name} not supported on this platform"
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in Shutdown.handler).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:4f7bef7b3ab433c04be28da10e8a00f8f403c7969b0545b814f8cc5397d61e70'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:4f7bef7b3ab433c04be28da10e8a00f8f403c7969b0545b814f8cc5397d61e70', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 71, 'function': 'Shutdown._register_handler'}, 'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 71, 'function': 'handler', 'agent_name': 'Shutdown', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                _lineaje_payload = _gr_client.enforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            self.logger.warning(
                _lineaje_payload
            )  # e.g. Windows

    async def start_auto_shutdown(self):
        _lineaje_payload = "Reached idle timeout, starting shutdown..."
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in Shutdown.start_auto_shutdown).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:71ca589af71cab1405fe4250a5f7c50614ebceb493dd2e26e570782e832212c6'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:71ca589af71cab1405fe4250a5f7c50614ebceb493dd2e26e570782e832212c6', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 76, 'function': 'Shutdown.start_auto_shutdown'}, 'file': 'packages/@n8n/task-runner-python/src/shutdown.py', 'line': 76, 'function': 'start_auto_shutdown', 'agent_name': 'Shutdown', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        self.logger.info(_lineaje_payload)
        await self.start_shutdown(3)  # no tasks so no grace period

    async def _perform_shutdown(self):
        await self.task_runner.stop()

        if self.health_check_server:
            await self.health_check_server.stop()

        if self.sentry:
            self.sentry.shutdown()
