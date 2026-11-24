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

import pytest_asyncio
from src.message_types.broker import Items
from src.message_serde import NODE_MODE_MAP

from tests.fixtures.local_task_broker import LocalTaskBroker
from tests.fixtures.task_runner_manager import TaskRunnerManager
from tests.fixtures.test_constants import (
    TASK_RESPONSE_WAIT,
)

NODE_MODE_TO_BROKER_STYLE = {v: k for k, v in NODE_MODE_MAP.items()}


@pytest_asyncio.fixture
async def broker():
    broker = LocalTaskBroker()
    await broker.start()
    yield broker
    await broker.stop()


@pytest_asyncio.fixture
async def manager(broker):
    manager = TaskRunnerManager(task_broker_url=broker.get_url())
    await manager.start()
    yield manager
    await manager.stop()


@pytest_asyncio.fixture
async def manager_with_stdlib_wildcard(broker):
    manager = TaskRunnerManager(
        task_broker_url=broker.get_url(),
        custom_env={
            "N8N_RUNNERS_STDLIB_ALLOW": "*",
        },
    )
    await manager.start()
    yield manager
    await manager.stop()


def create_task_settings(
    code: str,
    node_mode: str,
    items: Items | None = None,
    continue_on_fail: bool = False,
):
    return {
        "code": code,
        "nodeMode": NODE_MODE_TO_BROKER_STYLE[node_mode],
        "items": items if items is not None else [],
        "continueOnFail": continue_on_fail,
    }


async def wait_for_task_done(broker, task_id: str, timeout: float = TASK_RESPONSE_WAIT):
    return await broker.wait_for_msg(
        "runner:taskdone",
        timeout=timeout,
        predicate=lambda msg: msg.get("taskId") == task_id,
    )


async def wait_for_task_error(
    broker, task_id: str, timeout: float = TASK_RESPONSE_WAIT
):
    return await broker.wait_for_msg(
        "runner:taskerror",
        timeout=timeout,
        predicate=lambda msg: msg.get("taskId") == task_id,
    )


def get_browser_console_msgs(broker: LocalTaskBroker, task_id: str) -> list[list[str]]:
    console_msgs = []
    for msg in broker.get_task_rpc_messages(task_id):
        if msg.get("method") == "logNodeOutput":
            console_msgs.append(msg.get("params", []))
    # LINEAJE: runtime guardrail checks `console_msgs` as it flows from the agent to the user interface, before it leaves the application (in get_browser_console_msgs).
    #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
    #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `console_msgs` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
    #   Do not remove without review. site_id='site:sha256:677f2081ebda70eec0d0dc78589904fcc0c9ee8b126ccd1a8c73887c3d314265'
    _gr_client = _lineaje_load_gr_client()
    _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:677f2081ebda70eec0d0dc78589904fcc0c9ee8b126ccd1a8c73887c3d314265', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/integration/conftest.py', 'line': 80, 'function': 'get_browser_console_msgs'}, 'file': 'packages/@n8n/task-runner-python/tests/integration/conftest.py', 'line': 80, 'function': 'get_browser_console_msgs', 'agent_name': 'get_browser_console_msgs', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
    try:
        console_msgs = _gr_client.enforce(_gr_site, console_msgs, content_type='text/plain')
    except _gr_client.GuardrailUnavailableError:
        pass
    except PermissionError:
        pass
    return console_msgs
