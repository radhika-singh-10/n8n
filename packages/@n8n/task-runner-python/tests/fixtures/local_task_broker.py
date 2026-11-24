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
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from aiohttp import web, web_ws
from src.nanoid import nanoid

from tests.fixtures.test_constants import (
    TASK_RESPONSE_WAIT,
    LOCAL_TASK_BROKER_WS_PATH,
)

TaskId = str
TaskSettings = dict[str, Any]
WebsocketMessage = dict[str, Any]


@dataclass
class ActiveTask:
    settings: TaskSettings


class LocalTaskBroker:
    def __init__(self):
        self.port: int | None = None
        self.app = web.Application()
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self.connections: dict[str, web_ws.WebSocketResponse] = {}
        self.pending_messages: dict[str, asyncio.Queue[WebsocketMessage]] = {}
        self.received_messages: list[WebsocketMessage] = []
        self.active_tasks: dict[TaskId, ActiveTask] = {}
        self.task_settings: dict[TaskId, TaskSettings] = {}
        self.rpc_messages: dict[TaskId, list[dict]] = {}
        self.app.router.add_get(LOCAL_TASK_BROKER_WS_PATH, self.websocket_handler)

    async def start(self) -> None:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "localhost", 0)
        await self.site.start()
        assert self.site._server is not None
        self.port = self.site._server.sockets[0].getsockname()[1]
        _lineaje_payload = f"Local task broker started on port {self.port}"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in LocalTaskBroker.start).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:2562a4b7d681cd83d05422b87da10c235303733f811bcd3df6872e03122a37e8'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:2562a4b7d681cd83d05422b87da10c235303733f811bcd3df6872e03122a37e8', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 46, 'function': 'LocalTaskBroker.start'}, 'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 46, 'function': 'start', 'agent_name': 'LocalTaskBroker', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        print(_lineaje_payload)

    def get_url(self) -> str:
        return f"http://localhost:{self.port}"

    async def stop(self) -> None:
        for ws in self.connections.values():
            await ws.close()
        self.connections.clear()

        if self.site:
            await self.site.stop()

        if self.runner:
            await self.runner.cleanup()

    async def websocket_handler(self, request: web.Request) -> web_ws.WebSocketResponse:
        _lineaje_payload = f"WebSocket connection request from {request.remote}"
        # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in LocalTaskBroker.websocket_handler).
        #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:5bae15c9abdcfec4e7722ab9dc95ebd00a5838226c280a05b3e2ba999e786970'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:5bae15c9abdcfec4e7722ab9dc95ebd00a5838226c280a05b3e2ba999e786970', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 63, 'function': 'LocalTaskBroker.websocket_handler'}, 'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 63, 'function': 'websocket_handler', 'agent_name': 'LocalTaskBroker', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            _lineaje_payload = await _gr_client.aenforce(_gr_site, _lineaje_payload, content_type='application/json')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        print(_lineaje_payload)
        ws = web_ws.WebSocketResponse()
        await ws.prepare(request)
        connection_id = nanoid()
        self.connections[connection_id] = ws
        self.pending_messages[connection_id] = asyncio.Queue()

        sender_coroutine = asyncio.create_task(self._message_sender(connection_id, ws))

        try:
            await self.send_to_connection(connection_id, {"type": "broker:inforequest"})

            async for message in ws:
                if message.type == web_ws.WSMsgType.TEXT:
                    json_message = json.loads(message.data)
                    self.received_messages.append(json_message)
                    await self._handle_message(connection_id, json_message)
        finally:
            sender_coroutine.cancel()
            try:
                await sender_coroutine
            except asyncio.CancelledError:
                pass

            del self.connections[connection_id]
            del self.pending_messages[connection_id]

        # LINEAJE: runtime guardrail checks `ws` as it flows from the agent to the user interface, before it leaves the application (in LocalTaskBroker.websocket_handler).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `ws` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:f2a764a19534e83bbe69a12b5d7e74b354cf4e4ece8cf105c9e8767ff48179da'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:f2a764a19534e83bbe69a12b5d7e74b354cf4e4ece8cf105c9e8767ff48179da', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 90, 'function': 'LocalTaskBroker.websocket_handler'}, 'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 90, 'function': 'websocket_handler', 'agent_name': 'LocalTaskBroker', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            ws = await _gr_client.aenforce(_gr_site, ws, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return ws

    async def _message_sender(self, connection_id: str, ws: web_ws.WebSocketResponse):
        while True:
            message = await self.pending_messages[connection_id].get()
            # LINEAJE: runtime guardrail checks `message` as it flows from an external API to the agent, after the tool returns (in LocalTaskBroker._message_sender).
            #   Detected: this statement moves data from an external API to the agent with no runtime policy check (detection confidence: medium).
            #   Decision: the Guardrail evaluates the policies enabled for this boundary; sensitive values in `message` are masked in place; a policy violation is logged and the call continues (fail_mode=ALLOW_WITH_AUDIT). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:0bb84dbffeb6bfa3cb494b409b958430aa45e5475b47dadeb280ed0a1ba74c8f'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:0bb84dbffeb6bfa3cb494b409b958430aa45e5475b47dadeb280ed0a1ba74c8f', phase='post_tool', boundary={'source': 'external_endpoint', 'sink': 'agent_message'}, fail_mode='ALLOW_WITH_AUDIT', source_type='api', destination_type='agent', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 94, 'function': 'LocalTaskBroker._message_sender'}, 'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 94, 'function': '_message_sender', 'agent_name': 'LocalTaskBroker', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                message = await _gr_client.aenforce(_gr_site, message, content_type='application/json', variable_name='message', source_file=__file__, before_line=94)
            except _gr_client.GuardrailUnavailableError:
                pass
            # LINEAJE: runtime guardrail checks `message` as it flows from the agent to an external endpoint, before it leaves the application (in LocalTaskBroker._message_sender).
            #   Detected: this statement moves data from the agent to an external endpoint with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates the policies enabled for this boundary; sensitive values in `message` are masked in place; a policy violation is logged and the call continues (fail_mode=ALLOW_WITH_AUDIT). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:46c6b68669705efdd1ecbbe103268b4bf033e441f5479fd7da0c4aec80217f09'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:46c6b68669705efdd1ecbbe103268b4bf033e441f5479fd7da0c4aec80217f09', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'external_endpoint'}, fail_mode='ALLOW_WITH_AUDIT', source_type='agent', destination_type='external', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 95, 'function': 'LocalTaskBroker._message_sender'}, 'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 95, 'function': '_message_sender', 'agent_name': 'LocalTaskBroker', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                    __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                    raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                message = await _gr_client.aenforce(_gr_site, message, content_type='application/json', variable_name='message', source_file=__file__, before_line=95)
            except _gr_client.GuardrailUnavailableError:
                pass
            await ws.send_str(json.dumps(message))

    async def _handle_message(self, connection_id: str, message: WebsocketMessage):
        match message.get("type"):
            case "runner:info":
                await self.send_to_connection(
                    connection_id, {"type": "broker:runnerregistered"}
                )

            case "runner:taskoffer":
                pass  # Handled by send_task() which waits for them

            case "runner:taskaccepted":
                task_id = message.get("taskId")
                if task_id in self.task_settings:
                    await self.send_to_connection(
                        connection_id,
                        {
                            "type": "broker:tasksettings",
                            "taskId": task_id,
                            "settings": self.task_settings[task_id],
                        },
                    )

            case "runner:taskdone" | "runner:taskerror":
                task_id = message.get("taskId")
                if task_id in self.active_tasks:
                    del self.active_tasks[task_id]

            case "runner:rpc":
                task_id = message.get("taskId")
                if task_id:
                    if task_id not in self.rpc_messages:
                        self.rpc_messages[task_id] = []
                    self.rpc_messages[task_id].append(
                        {"method": message.get("name"), "params": message.get("params")}
                    )

    async def send_to_connection(self, connection_id: str, message: WebsocketMessage):
        if connection_id in self.pending_messages:
            await self.pending_messages[connection_id].put(message)

    async def send_task(
        self,
        task_id: TaskId,
        task_settings: TaskSettings,
    ):
        self.active_tasks[task_id] = ActiveTask(task_settings)
        self.task_settings[task_id] = task_settings

        offer = await self.wait_for_msg("runner:taskoffer", timeout=2.0)

        if offer:
            accept = {
                "type": "broker:taskofferaccept",
                "taskId": task_id,
                "offerId": offer.get("offerId"),
            }

            if self.connections:
                connection = next(iter(self.connections.keys()))
                await self.send_to_connection(connection, accept)

    async def cancel_task(self, task_id: TaskId, reason: str):
        cancel_message = {
            "type": "broker:taskcancel",
            "taskId": task_id,
            "reason": reason,
        }

        for connection_id in self.connections:
            await self.send_to_connection(connection_id, cancel_message)

    async def wait_for_msg(
        self,
        msg_type: str,
        timeout: float = TASK_RESPONSE_WAIT,
        predicate: Callable[[WebsocketMessage], bool] | None = None,
    ) -> WebsocketMessage | None:
        start_time = asyncio.get_running_loop().time()

        while asyncio.get_running_loop().time() - start_time < timeout:
            for msg in self.received_messages:
                if msg.get("type") == msg_type:
                    if predicate is None or predicate(msg):
                        # LINEAJE: runtime guardrail checks `msg` as it flows from the agent to the user interface, before it leaves the application (in LocalTaskBroker.wait_for_msg).
                        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
                        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `msg` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
                        #   Do not remove without review. site_id='site:sha256:f01b690ef85dee03764bb0c2e2488d08d7ec9260d3ca2688e98045c515ec946c'
                        _gr_client = _lineaje_load_gr_client()
                        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:f01b690ef85dee03764bb0c2e2488d08d7ec9260d3ca2688e98045c515ec946c', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 180, 'function': 'LocalTaskBroker.wait_for_msg'}, 'file': 'packages/@n8n/task-runner-python/tests/fixtures/local_task_broker.py', 'line': 180, 'function': 'wait_for_msg', 'agent_name': 'LocalTaskBroker', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
                        try:
                            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                            msg = await _gr_client.aenforce(_gr_site, msg, content_type='text/plain')
                        except _gr_client.GuardrailUnavailableError:
                            pass
                        except PermissionError:
                            pass
                        return msg

            await asyncio.sleep(0.1)

        return None

    def get_messages_of_type(self, msg_type: str) -> list[WebsocketMessage]:
        return [msg for msg in self.received_messages if msg.get("type") == msg_type]

    def get_task_rpc_messages(self, task_id: TaskId) -> list[dict]:
        return self.rpc_messages.get(task_id, [])
