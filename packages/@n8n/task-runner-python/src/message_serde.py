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

import json
from dataclasses import asdict
from typing import cast

from src.message_types.broker import NodeMode, TaskSettings
from src.constants import (
    BROKER_INFO_REQUEST,
    BROKER_RUNNER_REGISTERED,
    BROKER_TASK_CANCEL,
    BROKER_TASK_OFFER_ACCEPT,
    BROKER_TASK_SETTINGS,
    BROKER_RPC_RESPONSE,
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
)


NODE_MODE_MAP = {
    "runOnceForAllItems": "all_items",
    "runOnceForEachItem": "per_item",
}


def _get_node_mode(node_mode_str: str) -> NodeMode:
    if node_mode_str not in NODE_MODE_MAP:
        raise ValueError(f"Unknown nodeMode: {node_mode_str}")
    return cast(NodeMode, NODE_MODE_MAP[node_mode_str])


def _parse_task_settings(d: dict) -> BrokerTaskSettings:
    try:
        # required
        task_id = d["taskId"]
        settings_dict = d["settings"]
        code = settings_dict["code"]
        node_mode = _get_node_mode(settings_dict["nodeMode"])
        items = settings_dict["items"]

        # optional
        continue_on_fail = settings_dict.get("continueOnFail", False)
        workflow_name = settings_dict.get("workflowName", "Unknown")
        workflow_id = settings_dict.get("workflowId", "Unknown")
        node_name = settings_dict.get("nodeName", "Unknown")
        node_id = settings_dict.get("nodeId", "Unknown")
    except KeyError as e:
        raise ValueError(f"Missing field in task settings message: {e}")

    return BrokerTaskSettings(
        task_id=task_id,
        settings=TaskSettings(
            code=code,
            node_mode=node_mode,
            continue_on_fail=continue_on_fail,
            items=items,
            workflow_name=workflow_name,
            workflow_id=workflow_id,
            node_name=node_name,
            node_id=node_id,
        ),
    )


def _parse_task_offer_accept(d: dict) -> BrokerTaskOfferAccept:
    try:
        task_id = d["taskId"]
        offer_id = d["offerId"]
    except KeyError as e:
        raise ValueError(f"Missing field in task offer acceptance message: {e}")

    return BrokerTaskOfferAccept(task_id=task_id, offer_id=offer_id)


def _parse_task_cancel(d: dict) -> BrokerTaskCancel:
    try:
        task_id = d["taskId"]
        reason = d["reason"]
    except KeyError as e:
        raise ValueError(f"Missing field in task cancel message: {e}")

    return BrokerTaskCancel(task_id=task_id, reason=reason)


def _parse_rpc_response(d: dict) -> BrokerRpcResponse:
    try:
        call_id = d["callId"]
        task_id = d["taskId"]
        status = d["status"]
    except KeyError as e:
        raise ValueError(f"Missing field in RPC response message: {e}")

    return BrokerRpcResponse(call_id, task_id, status)


MESSAGE_TYPE_MAP = {
    BROKER_INFO_REQUEST: lambda _: BrokerInfoRequest(),
    BROKER_RUNNER_REGISTERED: lambda _: BrokerRunnerRegistered(),
    BROKER_TASK_OFFER_ACCEPT: _parse_task_offer_accept,
    BROKER_TASK_SETTINGS: _parse_task_settings,
    BROKER_TASK_CANCEL: _parse_task_cancel,
    BROKER_RPC_RESPONSE: _parse_rpc_response,
}


class MessageSerde:
    """Responsible for deserializing incoming messages and serializing outgoing messages."""

    @staticmethod
    def deserialize_broker_message(data: str) -> BrokerMessage:
        message_dict = json.loads(data)
        message_type = message_dict.get("type")

        if message_type not in MESSAGE_TYPE_MAP:
            raise ValueError(f"Unknown message type: {message_type}")

        return MESSAGE_TYPE_MAP[message_type](message_dict)

    @staticmethod
    def serialize_runner_message(message: RunnerMessage) -> str:
        data = asdict(message)
        camel_case_data = {
            MessageSerde._snake_to_camel_case(k): v for k, v in data.items()
        }
        # LINEAJE: runtime guardrail checks `camel_case_data` as it flows from the agent to an external endpoint, before it leaves the application (in MessageSerde.serialize_runner_message).
        #   Detected: this statement moves data from the agent to an external endpoint with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates the policies enabled for this boundary; sensitive values in `camel_case_data` are masked in place; a policy violation is logged and the call continues (fail_mode=ALLOW_WITH_AUDIT). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:2f7e9a91dd14c4280214f3ddec9b9d2f265b06eaf1c5dbe3a1b21819a1e8f542'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:2f7e9a91dd14c4280214f3ddec9b9d2f265b06eaf1c5dbe3a1b21819a1e8f542', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'external_endpoint'}, fail_mode='ALLOW_WITH_AUDIT', source_type='agent', destination_type='external', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/message_serde.py', 'line': 131, 'function': 'MessageSerde.serialize_runner_message'}, 'file': 'packages/@n8n/task-runner-python/src/message_serde.py', 'line': 131, 'function': 'serialize_runner_message', 'agent_name': 'MessageSerde', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            camel_case_data = _gr_client.enforce(_gr_site, camel_case_data, content_type='application/json', variable_name='camel_case_data', source_file=__file__, before_line=131)
        except _gr_client.GuardrailUnavailableError:
            pass
        return json.dumps(camel_case_data)

    @staticmethod
    def _snake_to_camel_case(snake_case_str: str) -> str:
        parts = snake_case_str.split("_")
        return parts[0] + "".join(word.capitalize() for word in parts[1:])
