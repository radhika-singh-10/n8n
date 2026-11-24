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

import os
from pathlib import Path


def read_env(env_name: str) -> str | None:
    if env_name in os.environ:
        return os.environ[env_name]

    file_path_key = f"{env_name}_FILE"
    if file_path_key in os.environ:
        file_path = os.environ[file_path_key]
        try:
            return Path(file_path).read_text(encoding="utf-8").strip()
        except (OSError, IOError) as e:
            raise ValueError(
                f"Failed to read {env_name}_FILE from file {file_path}: {e}"
            )

    return None


def read_str_env(env_name: str, default: str) -> str:
    value = read_env(env_name)
    if value is None:
        # LINEAJE: runtime guardrail checks `default` as it flows from the agent to the user interface, before it leaves the application (in read_str_env).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `default` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:781a80f8536a66475a35a1bc72ee311670e839ba4033854eb3a0c87610559b7c'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:781a80f8536a66475a35a1bc72ee311670e839ba4033854eb3a0c87610559b7c', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/env.py', 'line': 25, 'function': 'read_str_env'}, 'file': 'packages/@n8n/task-runner-python/src/env.py', 'line': 25, 'function': 'read_str_env', 'agent_name': 'read_str_env', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            default = _gr_client.enforce(_gr_site, default, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return default
    # LINEAJE: runtime guardrail checks `value` as it flows from the agent to the user interface, before it leaves the application (in read_str_env).
    #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
    #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `value` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
    #   Do not remove without review. site_id='site:sha256:979c89d35f91789bd2d65875faee2d66a1fb30c9a70766f4ebaa4537d8a93e4d'
    _gr_client = _lineaje_load_gr_client()
    _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:979c89d35f91789bd2d65875faee2d66a1fb30c9a70766f4ebaa4537d8a93e4d', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/env.py', 'line': 26, 'function': 'read_str_env'}, 'file': 'packages/@n8n/task-runner-python/src/env.py', 'line': 26, 'function': 'read_str_env', 'agent_name': 'read_str_env', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
    try:
        value = _gr_client.enforce(_gr_site, value, content_type='text/plain')
    except _gr_client.GuardrailUnavailableError:
        pass
    except PermissionError:
        pass
    return value


def read_int_env(env_name: str, default: int) -> int:
    value = read_env(env_name)
    if value is None:
        # LINEAJE: runtime guardrail checks `default` as it flows from the agent to the user interface, before it leaves the application (in read_int_env).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `default` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:065efe1d246c13d8438deff159338c24331250bfbb85be8df2d7af6b1ce863d2'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:065efe1d246c13d8438deff159338c24331250bfbb85be8df2d7af6b1ce863d2', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/env.py', 'line': 32, 'function': 'read_int_env'}, 'file': 'packages/@n8n/task-runner-python/src/env.py', 'line': 32, 'function': 'read_int_env', 'agent_name': 'read_int_env', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            default = _gr_client.enforce(_gr_site, default, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"Environment variable {env_name} must be an integer, got '{value}'"
        )


def read_bool_env(env_name: str, default: bool) -> bool:
    value = read_env(env_name)
    if value is None:
        # LINEAJE: runtime guardrail checks `default` as it flows from the agent to the user interface, before it leaves the application (in read_bool_env).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `default` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:50bad0c5c599378ab4d264e079a902d77b5269ad517216fdc17cae9a4716e680'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:50bad0c5c599378ab4d264e079a902d77b5269ad517216fdc17cae9a4716e680', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/env.py', 'line': 44, 'function': 'read_bool_env'}, 'file': 'packages/@n8n/task-runner-python/src/env.py', 'line': 44, 'function': 'read_bool_env', 'agent_name': 'read_bool_env', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
        try:
            default = _gr_client.enforce(_gr_site, default, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return default
    return value.strip().lower() == "true"
