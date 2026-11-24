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
import time
from typing import Any, Callable, Awaitable
from dataclasses import dataclass
from urllib.parse import urlparse
import websockets
import random
from src.errors import TaskCancelledError


from src.config.task_runner_config import TaskRunnerConfig
from src.errors import (
    NoIdleTimeoutHandlerError,
    TaskMissingError,
    WebsocketConnectionError,
)
from src.message_types.broker import TaskSettings
from src.nanoid import nanoid

from src.constants import (
    RUNNER_NAME,
    TASK_REJECTED_REASON_AT_CAPACITY,
    TASK_REJECTED_REASON_OFFER_EXPIRED,
    TASK_TYPE_PYTHON,
    OFFER_INTERVAL,
    OFFER_VALIDITY,
    OFFER_VALIDITY_MAX_JITTER,
    OFFER_VALIDITY_LATENCY_BUFFER,
    TASK_BROKER_WS_PATH,
    RPC_BROWSER_CONSOLE_LOG_METHOD,
    LOG_TASK_COMPLETE,
    LOG_TASK_CANCEL,
    LOG_TASK_CANCEL_UNKNOWN,
    LOG_TASK_CANCEL_WAITING,
)
from src.message_types import (
    BrokerMessage,
    RunnerMessage,
    BrokerInfoRequest,
    BrokerRunnerRegistered,
    BrokerTaskOfferAccept,
    BrokerTaskSettings,
    BrokerTaskCancel,
    BrokerRpcResponse,
    RunnerInfo,
    RunnerTaskOffer,
    RunnerTaskAccepted,
    RunnerTaskRejected,
    RunnerTaskDone,
    RunnerTaskError,
    RunnerRpcCall,
)
from src.message_serde import MessageSerde
from src.task_state import TaskState, TaskStatus
from src.task_executor import TaskExecutor
from src.task_analyzer import TaskAnalyzer
from src.config.security_config import SecurityConfig


@dataclass
class TaskOffer:
    offer_id: str
    valid_until: float

    @property
    def has_expired(self) -> bool:
        return time.time() > self.valid_until


class TaskRunner:
    def __init__(
        self,
        config: TaskRunnerConfig,
    ):
        self.runner_id = nanoid()
        self.name = RUNNER_NAME
        self.config = config

        self.websocket_connection: Any | None = None
        self.can_send_offers = False

        self.open_offers: dict[str, TaskOffer] = {}
        self.running_tasks: dict[str, TaskState] = {}

        self.offers_coroutine: asyncio.Task | None = None
        self.serde = MessageSerde()
        self.executor = TaskExecutor()
        self.security_config = SecurityConfig(
            stdlib_allow=config.stdlib_allow,
            external_allow=config.external_allow,
            builtins_deny=config.builtins_deny,
        )
        self.analyzer = TaskAnalyzer(self.security_config)
        self.logger = logging.getLogger(__name__)

        self.idle_coroutine: asyncio.Task | None = None
        self.on_idle_timeout: Callable[[], Awaitable[None]] | None = None
        self.last_activity_time = time.time()
        self.is_shutting_down = False

        self.task_broker_uri = config.task_broker_uri
        websocket_host = urlparse(config.task_broker_uri).netloc
        self.websocket_url = (
            f"ws://{websocket_host}{TASK_BROKER_WS_PATH}?id={self.runner_id}"
        )

    @property
    def running_tasks_count(self) -> int:
        return len(self.running_tasks)

    async def start(self) -> None:
        if self.config.is_auto_shutdown_enabled and not self.on_idle_timeout:
            raise NoIdleTimeoutHandlerError(self.config.auto_shutdown_timeout)

        headers = {"Authorization": f"Bearer {self.config.grant_token}"}

        while not self.is_shutting_down:
            try:
                self.websocket_connection = await websockets.connect(
                    self.websocket_url,
                    additional_headers=headers,
                    max_size=self.config.max_payload_size,
                )
                _lineaje_payload = "Connected to broker"
                # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner.start).
                #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
                #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:e5b9f7e10111a1dc7d67fe4b7ad877f6d31090c8a6a3314def3525acabd264f2'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:e5b9f7e10111a1dc7d67fe4b7ad877f6d31090c8a6a3314def3525acabd264f2', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 125, 'function': 'TaskRunner.start'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 125, 'function': 'start', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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
                await self._listen_for_messages()

            except Exception:
                raise WebsocketConnectionError(self.task_broker_uri)

            if not self.is_shutting_down:
                self.websocket_connection = None
                self.can_send_offers = False
                await self._cancel_coroutine(self.offers_coroutine)
                await self._cancel_coroutine(self.idle_coroutine)
                await asyncio.sleep(5)

    async def _cancel_coroutine(self, coroutine: asyncio.Task | None) -> None:
        if coroutine and not coroutine.done():
            coroutine.cancel()
            try:
                await coroutine
            except asyncio.CancelledError:
                pass

    # ========== Shutdown ==========

    async def stop(self) -> None:
        self.is_shutting_down = True
        self.can_send_offers = False

        await self._cancel_coroutine(self.offers_coroutine)
        await self._cancel_coroutine(self.idle_coroutine)

        await self._wait_for_tasks()
        await self._terminate_tasks()

        if self.websocket_connection:
            await self.websocket_connection.close()
            _lineaje_payload = "Disconnected from broker"
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner.stop).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:5914466b68cf2c9650bd46519bc8f6e64fe236bfb7ca08cb70b258f02589280d'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:5914466b68cf2c9650bd46519bc8f6e64fe236bfb7ca08cb70b258f02589280d', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 160, 'function': 'TaskRunner.stop'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 160, 'function': 'stop', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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

        _lineaje_payload = "Runner stopped"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner.stop).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:b9bff55da18e2f3577eb3021207f015748e7909e098364df41344340bbd5f37e'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:b9bff55da18e2f3577eb3021207f015748e7909e098364df41344340bbd5f37e', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 162, 'function': 'TaskRunner.stop'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 162, 'function': 'stop', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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

    async def _wait_for_tasks(self):
        if not self.running_tasks:
            return

        timeout = self.config.graceful_shutdown_timeout
        _lineaje_payload = f"Waiting for {self.running_tasks_count} tasks to complete (timeout: {timeout}s)..."
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._wait_for_tasks).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:4d8b7a08a59463deab3603ac01294bb00a59d70d4e8d36dfca7101f322294b9a'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:4d8b7a08a59463deab3603ac01294bb00a59d70d4e8d36dfca7101f322294b9a', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 169, 'function': 'TaskRunner._wait_for_tasks'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 169, 'function': '_wait_for_tasks', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        self.logger.debug(
            _lineaje_payload
        )

        start_time = time.time()
        while self.running_tasks and (time.time() - start_time) < timeout:
            await asyncio.sleep(0.5)

        if self.running_tasks:
            _lineaje_payload = f"Timed out waiting for {self.running_tasks_count} tasks to complete"
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._wait_for_tasks).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:b82662f57407fbaee9daa1b6c0348b6dd85616c863f572aed14d09a54c4a4b6e'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:b82662f57407fbaee9daa1b6c0348b6dd85616c863f572aed14d09a54c4a4b6e', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 178, 'function': 'TaskRunner._wait_for_tasks'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 178, 'function': '_wait_for_tasks', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            self.logger.warning(
                _lineaje_payload
            )

    async def _terminate_tasks(self):
        if not self.running_tasks:
            return

        _lineaje_payload = f"Terminating {self.running_tasks_count} tasks..."
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._terminate_tasks).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:2569645a7a0607befd795f28db660818612fe198b1400ffa66aa76985d5e335d'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:2569645a7a0607befd795f28db660818612fe198b1400ffa66aa76985d5e335d', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 186, 'function': 'TaskRunner._terminate_tasks'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 186, 'function': '_terminate_tasks', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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

        tasks_to_terminate = [
            asyncio.to_thread(self.executor.stop_process, task_state.process)
            for task_state in self.running_tasks.values()
            if task_state.process
        ]

        if tasks_to_terminate:
            await asyncio.gather(*tasks_to_terminate, return_exceptions=True)

        self.running_tasks.clear()

        _lineaje_payload = "Terminated tasks"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._terminate_tasks).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:1f797713f1fe6dc9f9896d5785f5ff4989e8f327e5a37148700b4a7faf59ccdc'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:1f797713f1fe6dc9f9896d5785f5ff4989e8f327e5a37148700b4a7faf59ccdc', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 199, 'function': 'TaskRunner._terminate_tasks'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 199, 'function': '_terminate_tasks', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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

    # ========== Messages ==========

    async def _listen_for_messages(self) -> None:
        if self.websocket_connection is None:
            raise WebsocketConnectionError(self.task_broker_uri)

        async for raw_message in self.websocket_connection:
            try:
                message = self.serde.deserialize_broker_message(raw_message)
                await self._handle_message(message)
            except websockets.ConnectionClosedOK:
                break
            except Exception as e:
                _lineaje_payload = f"Error handling message: {e}"
                # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._listen_for_messages).
                #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
                #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:57ed5c3b2520a9673e345e28cad5426268a80a34debabdf6e2f319e08b9c5615'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:57ed5c3b2520a9673e345e28cad5426268a80a34debabdf6e2f319e08b9c5615', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 214, 'function': 'TaskRunner._listen_for_messages'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 214, 'function': '_listen_for_messages', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
                try:
                    if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                        __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                        raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                    _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
                except _gr_client.GuardrailUnavailableError:
                    pass
                except PermissionError:
                    pass
                self.logger.error(_lineaje_payload)

    async def _handle_message(self, message: BrokerMessage) -> None:
        match message:
            case BrokerInfoRequest():
                await self._handle_info_request()
            case BrokerRunnerRegistered():
                await self._handle_runner_registered()
            case BrokerTaskOfferAccept():
                await self._handle_task_offer_accept(message)
            case BrokerTaskSettings():
                await self._handle_task_settings(message)
            case BrokerTaskCancel():
                await self._handle_task_cancel(message)
            case BrokerRpcResponse():
                pass  # currently only logging, already handled by browser
            case _:
                _lineaje_payload = f"Unhandled message type: {type(message)}"
                # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._handle_message).
                #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
                #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:6d9236b6e5702c31ac552d5c807104dfd6a8e2c01ad53fdc37a5ffefa17a94d8'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:6d9236b6e5702c31ac552d5c807104dfd6a8e2c01ad53fdc37a5ffefa17a94d8', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 231, 'function': 'TaskRunner._handle_message'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 231, 'function': '_handle_message', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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

    async def _handle_info_request(self) -> None:
        response = RunnerInfo(name=self.name, types=[TASK_TYPE_PYTHON])
        await self._send_message(response)

    async def _handle_runner_registered(self) -> None:
        self.can_send_offers = True
        self.offers_coroutine = asyncio.create_task(self._send_offers_loop())
        _lineaje_payload = "Registered with broker"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._handle_runner_registered).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:347037aa7616b783c398012e2268777421519e538c3f8512fe1105b3e429c587'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:347037aa7616b783c398012e2268777421519e538c3f8512fe1105b3e429c587', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 240, 'function': 'TaskRunner._handle_runner_registered'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 240, 'function': '_handle_runner_registered', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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
        self._reset_idle_timer()

    async def _handle_task_offer_accept(self, message: BrokerTaskOfferAccept) -> None:
        offer = self.open_offers.get(message.offer_id)

        if offer is None or offer.has_expired:
            response = RunnerTaskRejected(
                task_id=message.task_id,
                reason=TASK_REJECTED_REASON_OFFER_EXPIRED,
            )
            await self._send_message(response)
            return

        if self.running_tasks_count >= self.config.max_concurrency:
            response = RunnerTaskRejected(
                task_id=message.task_id,
                reason=TASK_REJECTED_REASON_AT_CAPACITY,
            )
            await self._send_message(response)
            return

        del self.open_offers[message.offer_id]

        task_state = TaskState(message.task_id)
        self.running_tasks[message.task_id] = task_state

        response = RunnerTaskAccepted(task_id=message.task_id)
        await self._send_message(response)
        _lineaje_payload = f"Accepted task {message.task_id}"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._handle_task_offer_accept).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:12bd0e6a0abb0315a09234e47afac5668f235605334115388e49fbf4103ea5b6'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:12bd0e6a0abb0315a09234e47afac5668f235605334115388e49fbf4103ea5b6', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 269, 'function': 'TaskRunner._handle_task_offer_accept'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 269, 'function': '_handle_task_offer_accept', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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
        self._reset_idle_timer()

    async def _handle_task_settings(self, message: BrokerTaskSettings) -> None:
        task_state = self.running_tasks.get(message.task_id)
        if task_state is None:
            raise TaskMissingError(message.task_id)

        if task_state.status != TaskStatus.WAITING_FOR_SETTINGS:
            _lineaje_payload = f"Received settings for task but it is already {task_state.status}. Discarding message."
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._handle_task_settings).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:dd957ffc1ff1d1f85c78e931e5ecdd8e02415c49fae3d847ccf2431e5cecf259'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:dd957ffc1ff1d1f85c78e931e5ecdd8e02415c49fae3d847ccf2431e5cecf259', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 278, 'function': 'TaskRunner._handle_task_settings'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 278, 'function': '_handle_task_settings', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            self.logger.warning(
                _lineaje_payload
            )
            return

        task_state.workflow_name = message.settings.workflow_name
        task_state.workflow_id = message.settings.workflow_id
        task_state.node_name = message.settings.node_name
        task_state.node_id = message.settings.node_id

        task_state.status = TaskStatus.RUNNING
        asyncio.create_task(self._execute_task(message.task_id, message.settings))
        _lineaje_payload = f"Received task {message.task_id}"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._handle_task_settings).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:e73f59c44cbdbb0406ad5211ea8d2802bf7bcfdc0c492cba156bf85a34c562d8'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:e73f59c44cbdbb0406ad5211ea8d2802bf7bcfdc0c492cba156bf85a34c562d8', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 290, 'function': 'TaskRunner._handle_task_settings'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 290, 'function': '_handle_task_settings', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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

    async def _execute_task(self, task_id: str, task_settings: TaskSettings) -> None:
        start_time = time.time()

        try:
            task_state = self.running_tasks.get(task_id)

            if task_state is None:
                raise TaskMissingError(task_id)

            self.analyzer.validate(task_settings.code)

            process, queue = self.executor.create_process(
                code=task_settings.code,
                node_mode=task_settings.node_mode,
                items=task_settings.items,
                security_config=self.security_config,
            )

            task_state.process = process

            result, print_args, result_size_bytes = await asyncio.to_thread(
                self.executor.execute_process,
                process=process,
                queue=queue,
                task_timeout=self.config.task_timeout,
                continue_on_fail=task_settings.continue_on_fail,
            )

            for print_args_per_call in print_args:
                await self._send_rpc_message(
                    task_id, RPC_BROWSER_CONSOLE_LOG_METHOD, print_args_per_call
                )

            response = RunnerTaskDone(task_id=task_id, data={"result": result})
            await self._send_message(response)

            _lineaje_payload = (LOG_TASK_COMPLETE.format(
                    task_id=task_id,
                    duration=self._get_duration(start_time),
                    result_size=self._get_result_size(result_size_bytes),
                    **task_state.context(),
                ))
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._execute_task).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:9a3df35a103c61e6ec3888d35a64b14681263cbb836b4549c17582b23220716c'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:9a3df35a103c61e6ec3888d35a64b14681263cbb836b4549c17582b23220716c', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 328, 'function': 'TaskRunner._execute_task'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 328, 'function': '_execute_task', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            self.logger.info(
                _lineaje_payload
            )

        except TaskCancelledError as e:
            response = RunnerTaskError(task_id=task_id, error={"message": str(e)})
            await self._send_message(response)

        except SyntaxError as e:
            _lineaje_payload = f"Task {task_id} failed syntax validation"
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._execute_task).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:ccb81fabf998f41149f27654333482eb0026a2ed5d286339a060e9a93a94f36d'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:ccb81fabf998f41149f27654333482eb0026a2ed5d286339a060e9a93a94f36d', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 342, 'function': 'TaskRunner._execute_task'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 342, 'function': '_execute_task', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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
            error = {"message": str(e)}
            response = RunnerTaskError(task_id=task_id, error=error)
            await self._send_message(response)

        except Exception as e:
            _lineaje_payload = f"Task {task_id} failed"
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._execute_task).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:218437b2b589a41eaca9696fd310b152c0ead9e7cf36155d6f887ec735a43689'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:218437b2b589a41eaca9696fd310b152c0ead9e7cf36155d6f887ec735a43689', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 348, 'function': 'TaskRunner._execute_task'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 348, 'function': '_execute_task', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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
            error = {
                "message": getattr(e, "message", str(e)),
                "description": getattr(e, "description", ""),
            }
            response = RunnerTaskError(task_id=task_id, error=error)
            await self._send_message(response)

        finally:
            self.running_tasks.pop(task_id, None)
            self._reset_idle_timer()

    async def _handle_task_cancel(self, message: BrokerTaskCancel) -> None:
        task_id = message.task_id
        task_state = self.running_tasks.get(task_id)

        if task_state is None:
            _lineaje_payload = LOG_TASK_CANCEL_UNKNOWN.format(task_id=task_id)
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._handle_task_cancel).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:fe1ccb0dba082b397230229af5a6625a5073a29604b2e0d2ccc344eee0bf5575'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:fe1ccb0dba082b397230229af5a6625a5073a29604b2e0d2ccc344eee0bf5575', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 365, 'function': 'TaskRunner._handle_task_cancel'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 365, 'function': '_handle_task_cancel', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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
            return

        if task_state.status == TaskStatus.WAITING_FOR_SETTINGS:
            self.running_tasks.pop(task_id, None)
            _lineaje_payload = LOG_TASK_CANCEL_WAITING.format(task_id=task_id)
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._handle_task_cancel).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:df839ef127e32dc273109a18632375df0d4d8d8b22b15f4b26991212a172405f'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:df839ef127e32dc273109a18632375df0d4d8d8b22b15f4b26991212a172405f', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 370, 'function': 'TaskRunner._handle_task_cancel'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 370, 'function': '_handle_task_cancel', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
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
            await self._send_offers()
            return

        if task_state.status == TaskStatus.RUNNING:
            task_state.status = TaskStatus.ABORTING
            await asyncio.to_thread(self.executor.stop_process, task_state.process)
            _lineaje_payload = LOG_TASK_CANCEL.format(task_id=task_id, **task_state.context())
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._handle_task_cancel).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:67a4d2e63c35b11ec9e3e39952431516af70c2eb16ea5db7dcb4824c3e465ed7'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:67a4d2e63c35b11ec9e3e39952431516af70c2eb16ea5db7dcb4824c3e465ed7', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 377, 'function': 'TaskRunner._handle_task_cancel'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 377, 'function': '_handle_task_cancel', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            self.logger.info(
                _lineaje_payload
            )

    async def _send_rpc_message(self, task_id: str, method_name: str, params: list):
        message = RunnerRpcCall(
            call_id=nanoid(), task_id=task_id, name=method_name, params=params
        )

        await self._send_message(message)

    async def _send_message(self, message: RunnerMessage) -> None:
        if self.websocket_connection is None:
            raise WebsocketConnectionError(self.task_broker_uri)

        serialized = self.serde.serialize_runner_message(message)
        await self.websocket_connection.send(serialized)

    # ========== Formatting ==========

    def _get_duration(self, start_time: float) -> str:
        elapsed = time.time() - start_time

        if elapsed < 1:
            return f"{int(elapsed * 1000)}ms"

        if elapsed < 60:
            return f"{int(elapsed)}s"

        return f"{int(elapsed) // 60}m"

    def _get_result_size(self, size_bytes: int) -> str:
        if size_bytes < 1024:
            return f"{size_bytes} bytes"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes / (1024 * 1024):.1f} MB"

    # ========== Offers ==========

    async def _send_offers_loop(self) -> None:
        while self.can_send_offers:
            try:
                await self._send_offers()
                await asyncio.sleep(OFFER_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                _lineaje_payload = f"Error sending offers: {e}"
                # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskRunner._send_offers_loop).
                #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
                #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:e402dd003b3603fb8fe16bb34fedf4f6eaee1f2a506eedd50f65f3e281072be9'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:e402dd003b3603fb8fe16bb34fedf4f6eaee1f2a506eedd50f65f3e281072be9', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 426, 'function': 'TaskRunner._send_offers_loop'}, 'file': 'packages/@n8n/task-runner-python/src/task_runner.py', 'line': 426, 'function': '_send_offers_loop', 'agent_name': 'TaskRunner', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
                try:
                    if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                        __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                        raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                    _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
                except _gr_client.GuardrailUnavailableError:
                    pass
                except PermissionError:
                    pass
                self.logger.error(_lineaje_payload)

    async def _send_offers(self) -> None:
        if not self.can_send_offers:
            return

        expired_offer_ids = [
            offer_id
            for offer_id, offer in self.open_offers.items()
            if offer.has_expired
        ]

        for offer_id in expired_offer_ids:
            self.open_offers.pop(offer_id, None)

        offers_to_send = self.config.max_concurrency - (
            len(self.open_offers) + self.running_tasks_count
        )

        for _ in range(offers_to_send):
            offer_id = nanoid()

            valid_for_ms = OFFER_VALIDITY + random.randint(0, OFFER_VALIDITY_MAX_JITTER)

            valid_until = (
                time.time() + (valid_for_ms / 1000) + OFFER_VALIDITY_LATENCY_BUFFER
            )

            self.open_offers[offer_id] = TaskOffer(offer_id, valid_until)

            message = RunnerTaskOffer(
                offer_id=offer_id, task_type=TASK_TYPE_PYTHON, valid_for=valid_for_ms
            )

            await self._send_message(message)

    # ========== Inactivity ==========

    def _reset_idle_timer(self):
        """Reset idle timer when key event occurs, namely runner registration, task acceptance, and task completion or failure."""

        if not self.config.is_auto_shutdown_enabled:
            return

        self.last_activity_time = time.time()

        if self.idle_coroutine and not self.idle_coroutine.done():
            self.idle_coroutine.cancel()

        self.idle_coroutine = asyncio.create_task(self._idle_timer_coroutine())

    async def _idle_timer_coroutine(self):
        try:
            await asyncio.sleep(self.config.auto_shutdown_timeout)

            if self.running_tasks_count > 0:
                return

            assert self.on_idle_timeout is not None  # validated at start()

            await self.on_idle_timeout()
        except asyncio.CancelledError:
            pass
