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
import textwrap

import aiohttp
import pytest
from src.nanoid import nanoid

from tests.integration.conftest import create_task_settings


@pytest.mark.asyncio
async def test_health_check_server_responds(broker, manager):
    health_check_url = manager.get_health_check_url()
    async with aiohttp.ClientSession() as session:
        for _ in range(10):
            try:
                response = await session.get(health_check_url)
                # LINEAJE: runtime guardrail checks `response` as it flows from an external API to the agent, after the tool returns (in test_health_check_server_responds).
                #   Detected: this statement moves data from an external API to the agent with no runtime policy check (detection confidence: low).
                #   Decision: the Guardrail evaluates the policies enabled for this boundary; sensitive values in `response` are masked in place; a policy violation is logged and the call continues (fail_mode=ALLOW_WITH_AUDIT). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:05461f64e13413905adc448beac72c97defc05144c366b65eb3460b93ef6ac39'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:05461f64e13413905adc448beac72c97defc05144c366b65eb3460b93ef6ac39', phase='post_tool', boundary={'source': 'external_endpoint', 'sink': 'agent_message'}, fail_mode='ALLOW_WITH_AUDIT', source_type='api', destination_type='agent', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/integration/test_health_check.py', 'line': 17, 'function': 'test_health_check_server_responds'}, 'file': 'packages/@n8n/task-runner-python/tests/integration/test_health_check.py', 'line': 17, 'function': 'test_health_check_server_responds', 'agent_name': 'test_health_check_server_responds', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
                try:
                    if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                        __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                        raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
                    response = await _gr_client.aenforce(_gr_site, response, content_type='application/json', variable_name='response', source_file=__file__, before_line=17)
                except _gr_client.GuardrailUnavailableError:
                    pass
                if response.status == 200:
                    assert await response.text() == "OK"
                    return
            except aiohttp.ClientConnectionError:
                await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_health_check_server_ressponds_mid_execution(broker, manager):
    task_id = nanoid()
    code = textwrap.dedent("""
        for _ in range(10_000_000):
            pass
        return [{"result": "completed"}]
    """)
    task_settings = create_task_settings(code=code, node_mode="all_items")
    await broker.send_task(task_id=task_id, task_settings=task_settings)
    await asyncio.sleep(0.3)

    async with aiohttp.ClientSession() as session:
        response = await session.get(manager.get_health_check_url())
        # LINEAJE: runtime guardrail checks `response` as it flows from an external API to the agent, after the tool returns (in test_health_check_server_ressponds_mid_execution).
        #   Detected: this statement moves data from an external API to the agent with no runtime policy check (detection confidence: low).
        #   Decision: the Guardrail evaluates the policies enabled for this boundary; sensitive values in `response` are masked in place; a policy violation is logged and the call continues (fail_mode=ALLOW_WITH_AUDIT). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:bf38a7c1d0f740f348ce80fec049a28ea634c4bdf37f5df082bee6f40f019524'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:bf38a7c1d0f740f348ce80fec049a28ea634c4bdf37f5df082bee6f40f019524', phase='post_tool', boundary={'source': 'external_endpoint', 'sink': 'agent_message'}, fail_mode='ALLOW_WITH_AUDIT', source_type='api', destination_type='agent', components={'invocation': {'file': 'packages/@n8n/task-runner-python/tests/integration/test_health_check.py', 'line': 38, 'function': 'test_health_check_server_ressponds_mid_execution'}, 'file': 'packages/@n8n/task-runner-python/tests/integration/test_health_check.py', 'line': 38, 'function': 'test_health_check_server_ressponds_mid_execution', 'agent_name': 'test_health_check_server_ressponds_mid_execution', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            if not __import__('builtins').hasattr(_gr_client, 'aenforce'):
                __import__("logging").getLogger("lineaje.gr_client").error("gr_stub_client.py predates aenforce (STUB_API 5): this site is NOT enforced; replace gr_stub_client.py with the current companion")
                raise _gr_client.GuardrailUnavailableError("stale gr_stub_client.py: no aenforce")
            response = await _gr_client.aenforce(_gr_site, response, content_type='application/json', variable_name='response', source_file=__file__, before_line=38)
        except _gr_client.GuardrailUnavailableError:
            pass
        assert response.status == 200
        assert await response.text() == "OK"
