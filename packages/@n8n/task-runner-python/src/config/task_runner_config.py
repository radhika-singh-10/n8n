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

from dataclasses import dataclass

from src.env import read_int_env, read_str_env
from src.errors import ConfigurationError
from src.constants import (
    BUILTINS_DENY_DEFAULT,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MAX_PAYLOAD_SIZE,
    DEFAULT_TASK_BROKER_URI,
    DEFAULT_TASK_TIMEOUT,
    DEFAULT_AUTO_SHUTDOWN_TIMEOUT,
    DEFAULT_SHUTDOWN_TIMEOUT,
    ENV_BUILTINS_DENY,
    ENV_EXTERNAL_ALLOW,
    ENV_GRANT_TOKEN,
    ENV_MAX_CONCURRENCY,
    ENV_MAX_PAYLOAD_SIZE,
    ENV_STDLIB_ALLOW,
    ENV_TASK_BROKER_URI,
    ENV_TASK_TIMEOUT,
    ENV_AUTO_SHUTDOWN_TIMEOUT,
    ENV_GRACEFUL_SHUTDOWN_TIMEOUT,
)


def parse_allowlist(allowlist_str: str, list_name: str) -> set[str]:
    if not allowlist_str:
        return set()

    modules = {
        module
        for raw_module in allowlist_str.split(",")
        if (module := raw_module.strip())
    }

    if "*" in modules and len(modules) > 1:
        raise ConfigurationError(
            f"Wildcard '*' in {list_name} must be used alone, not with other modules. "
            f"Got: {', '.join(sorted(modules))}"
        )

    # LINEAJE: runtime guardrail checks `modules` as it flows from the agent to the user interface, before it leaves the application (in parse_allowlist).
    #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
    #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `modules` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
    #   Do not remove without review. site_id='site:sha256:0a6b54857097d2b7234f142abf884faed9350ea10e3b668439fa539742e5e099'
    _gr_client = _lineaje_load_gr_client()
    _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:0a6b54857097d2b7234f142abf884faed9350ea10e3b668439fa539742e5e099', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/config/task_runner_config.py', 'line': 42, 'function': 'parse_allowlist'}, 'file': 'packages/@n8n/task-runner-python/src/config/task_runner_config.py', 'line': 42, 'function': 'parse_allowlist', 'agent_name': 'parse_allowlist', 'agent_name_source': 'inferred_from_function', 'not_on_graph': True})
    try:
        modules = _gr_client.enforce(_gr_site, modules, content_type='text/plain')
    except _gr_client.GuardrailUnavailableError:
        pass
    except PermissionError:
        pass
    return modules


@dataclass
class TaskRunnerConfig:
    grant_token: str
    task_broker_uri: str
    max_concurrency: int
    max_payload_size: int
    task_timeout: int
    auto_shutdown_timeout: int
    graceful_shutdown_timeout: int
    stdlib_allow: set[str]
    external_allow: set[str]
    builtins_deny: set[str]

    @property
    def is_auto_shutdown_enabled(self) -> bool:
        return self.auto_shutdown_timeout > 0

    @classmethod
    def from_env(cls):
        grant_token = read_str_env(ENV_GRANT_TOKEN, "")
        if not grant_token:
            raise ConfigurationError(
                "Environment variable N8N_RUNNERS_GRANT_TOKEN is required"
            )

        task_timeout = read_int_env(ENV_TASK_TIMEOUT, DEFAULT_TASK_TIMEOUT)
        if task_timeout <= 0:
            raise ConfigurationError(
                f"Task timeout must be positive, got {task_timeout}"
            )

        auto_shutdown_timeout = read_int_env(
            ENV_AUTO_SHUTDOWN_TIMEOUT, DEFAULT_AUTO_SHUTDOWN_TIMEOUT
        )
        if auto_shutdown_timeout < 0:
            raise ConfigurationError(
                f"Auto shutdown timeout must be non-negative, got {auto_shutdown_timeout}"
            )

        graceful_shutdown_timeout = read_int_env(
            ENV_GRACEFUL_SHUTDOWN_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT
        )
        if graceful_shutdown_timeout <= 0:
            raise ConfigurationError(
                f"Graceful shutdown timeout must be positive, got {graceful_shutdown_timeout}"
            )

        return cls(
            grant_token=grant_token,
            task_broker_uri=read_str_env(ENV_TASK_BROKER_URI, DEFAULT_TASK_BROKER_URI),
            max_concurrency=read_int_env(ENV_MAX_CONCURRENCY, DEFAULT_MAX_CONCURRENCY),
            max_payload_size=read_int_env(
                ENV_MAX_PAYLOAD_SIZE, DEFAULT_MAX_PAYLOAD_SIZE
            ),
            task_timeout=task_timeout,
            auto_shutdown_timeout=auto_shutdown_timeout,
            graceful_shutdown_timeout=graceful_shutdown_timeout,
            stdlib_allow=parse_allowlist(
                read_str_env(ENV_STDLIB_ALLOW, ""), ENV_STDLIB_ALLOW
            ),
            external_allow=parse_allowlist(
                read_str_env(ENV_EXTERNAL_ALLOW, ""), ENV_EXTERNAL_ALLOW
            ),
            builtins_deny=set(
                module.strip()
                for module in read_str_env(
                    ENV_BUILTINS_DENY, BUILTINS_DENY_DEFAULT
                ).split(",")
            ),
        )
