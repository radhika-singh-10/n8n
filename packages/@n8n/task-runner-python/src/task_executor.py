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

from queue import Empty
import multiprocessing
import traceback
import textwrap
import json
import io
import os
import sys
import logging

from src.errors import (
    TaskCancelledError,
    TaskKilledError,
    TaskResultMissingError,
    TaskRuntimeError,
    TaskTimeoutError,
    TaskSubprocessFailedError,
    SecurityViolationError,
)
from src.import_validation import validate_module_import
from src.config.security_config import SecurityConfig

from src.message_types.broker import NodeMode, Items
from src.constants import (
    EXECUTOR_CIRCULAR_REFERENCE_KEY,
    EXECUTOR_USER_OUTPUT_KEY,
    EXECUTOR_ALL_ITEMS_FILENAME,
    EXECUTOR_PER_ITEM_FILENAME,
    SIGTERM_EXIT_CODE,
    SIGKILL_EXIT_CODE,
)
from typing import Any

from multiprocessing.context import ForkServerProcess
from multiprocessing import shared_memory

logger = logging.getLogger(__name__)

MULTIPROCESSING_CONTEXT = multiprocessing.get_context("forkserver")
MAX_PRINT_ARGS_ALLOWED = 100

PrintArgs = list[list[Any]]  # Args to all `print()` calls in a Python code task


class TaskExecutor:
    """Responsible for executing Python code tasks in isolated subprocesses."""

    @staticmethod
    def create_process(
        code: str,
        node_mode: NodeMode,
        items: Items,
        security_config: SecurityConfig,
    ):
        """Create a subprocess for executing a Python code task and a queue for communication."""

        fn = (
            TaskExecutor._all_items
            if node_mode == "all_items"
            else TaskExecutor._per_item
        )

        queue = MULTIPROCESSING_CONTEXT.Queue()
        process = MULTIPROCESSING_CONTEXT.Process(
            target=fn,
            args=(
                code,
                items,
                queue,
                security_config,
            ),
        )

        return process, queue

    @staticmethod
    def execute_process(
        process: ForkServerProcess,
        queue: multiprocessing.Queue,
        task_timeout: int,
        continue_on_fail: bool,
    ) -> tuple[list, PrintArgs, int]:
        """Execute a subprocess for a Python code task."""

        print_args: PrintArgs = []

        try:
            try:
                process.start()
            except (ProcessLookupError, ConnectionError, BrokenPipeError) as e:
                _lineaje_payload = f"Failed to start child process: {e}"
                # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskExecutor.execute_process).
                #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
                #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:6d77dcb4b097038062ff7b1a7120ca1f78546327d49e0cfbac2f9c31d284e146'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:6d77dcb4b097038062ff7b1a7120ca1f78546327d49e0cfbac2f9c31d284e146', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 91, 'function': 'TaskExecutor.execute_process'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 91, 'function': 'execute_process', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
                try:
                    _lineaje_payload = _gr_client.enforce(_gr_site, _lineaje_payload, content_type='application/json')
                except _gr_client.GuardrailUnavailableError:
                    pass
                except PermissionError:
                    pass
                logger.error(_lineaje_payload)
                raise TaskSubprocessFailedError(-1)

            process.join(timeout=task_timeout)

            if process.is_alive():
                TaskExecutor.stop_process(process)
                raise TaskTimeoutError(task_timeout)

            if process.exitcode == SIGTERM_EXIT_CODE:
                raise TaskCancelledError()

            if process.exitcode == SIGKILL_EXIT_CODE:
                raise TaskKilledError()

            if process.exitcode != 0:
                assert process.exitcode is not None
                raise TaskSubprocessFailedError(process.exitcode)

            try:
                returned = queue.get_nowait()
            except Empty:
                raise TaskResultMissingError()
            except EOFError as e:
                _lineaje_payload = f"Failed to retrieve results from child process: {e}"
                # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskExecutor.execute_process).
                #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
                #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:125e2485c76d9cb5def22f4430eb4080f9a617b24dd20b93d55f96f47ae82370'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:125e2485c76d9cb5def22f4430eb4080f9a617b24dd20b93d55f96f47ae82370', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 115, 'function': 'TaskExecutor.execute_process'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 115, 'function': 'execute_process', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
                try:
                    _lineaje_payload = _gr_client.enforce(_gr_site, _lineaje_payload, content_type='application/json')
                except _gr_client.GuardrailUnavailableError:
                    pass
                except PermissionError:
                    pass
                logger.error(_lineaje_payload)
                raise TaskResultMissingError()

            finally:
                queue.close()
                queue.join_thread()

            if "error" in returned:
                raise TaskRuntimeError(returned["error"])

            if "shm_name" not in returned:
                raise TaskResultMissingError()

            shm_name = returned["shm_name"]
            shm_size = returned["shm_size"]
            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                try:
                    json_str = bytes(shm.buf[:shm_size]).decode("utf-8")
                    result = json.loads(json_str)
                finally:
                    shm.close()
                    shm.unlink()
            except FileNotFoundError:
                raise TaskResultMissingError()

            print_args = returned.get("print_args", [])

            return result, print_args, shm_size

        except Exception as e:
            if continue_on_fail:
                return [{"json": {"error": str(e)}}], print_args, 0
            raise

    @staticmethod
    def stop_process(process: ForkServerProcess | None):
        """Stop a running subprocess, gracefully else force-killing."""

        if process is None or not process.is_alive():
            return

        try:
            process.terminate()
            process.join(timeout=1)  # 1s grace period

            if process.is_alive():
                process.kill()
                process.join()
        except (ProcessLookupError, ConnectionError, BrokenPipeError):
            # subprocess is dead or unreachable
            pass

    @staticmethod
    def _all_items(
        raw_code: str,
        items: Items,
        queue: multiprocessing.Queue,
        security_config: SecurityConfig,
    ):
        """Execute a Python code task in all-items mode."""

        os.environ.clear()

        TaskExecutor._sanitize_sys_modules(security_config)

        print_args: PrintArgs = []
        sys.stderr = stderr_capture = io.StringIO()

        try:
            wrapped_code = TaskExecutor._wrap_code(raw_code)
            compiled_code = compile(wrapped_code, EXECUTOR_ALL_ITEMS_FILENAME, "exec")

            globals = {
                "__builtins__": TaskExecutor._filter_builtins(security_config),
                "_items": items,
                "print": TaskExecutor._create_custom_print(print_args),
            }

            # LINEAJE: runtime guardrail checks `compiled_code` as it flows from the agent to system, at a security decision (in TaskExecutor._all_items).
            #   Detected: this statement moves data from the agent to system with no runtime policy check (detection confidence: medium).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_029; sensitive values in `compiled_code` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:2ee4f3caf402bf1149181b560614aa4ebec9b414da5c7033c97d779710c3a21b'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:2ee4f3caf402bf1149181b560614aa4ebec9b414da5c7033c97d779710c3a21b', phase='security_decision', boundary={'source': 'agent_message', 'sink': 'agent_message'}, fail_mode='BLOCK', source_type='agent', destination_type='system', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 194, 'function': 'TaskExecutor._all_items'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 194, 'function': '_all_items', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                compiled_code = _gr_client.enforce(_gr_site, compiled_code, content_type='application/json', variable_name='compiled_code', source_file=__file__, before_line=194)
            except _gr_client.GuardrailUnavailableError:
                pass
            exec(compiled_code, globals)

            result = globals[EXECUTOR_USER_OUTPUT_KEY]
            TaskExecutor._put_result(queue, result, print_args)

        except BaseException as e:
            TaskExecutor._put_error(queue, e, stderr_capture.getvalue(), print_args)

    @staticmethod
    def _per_item(
        raw_code: str,
        items: Items,
        queue: multiprocessing.Queue,
        security_config: SecurityConfig,
    ):
        """Execute a Python code task in per-item mode."""

        os.environ.clear()

        TaskExecutor._sanitize_sys_modules(security_config)

        print_args: PrintArgs = []
        sys.stderr = stderr_capture = io.StringIO()

        try:
            wrapped_code = TaskExecutor._wrap_code(raw_code)
            compiled_code = compile(wrapped_code, EXECUTOR_PER_ITEM_FILENAME, "exec")

            filtered_builtins = TaskExecutor._filter_builtins(security_config)
            custom_print = TaskExecutor._create_custom_print(print_args)

            result = []
            for index, item in enumerate(items):
                globals = {
                    "__builtins__": filtered_builtins,
                    "_item": item,
                    "print": custom_print,
                }

                # LINEAJE: runtime guardrail checks `compiled_code` as it flows from the agent to system, at a security decision (in TaskExecutor._per_item).
                #   Detected: this statement moves data from the agent to system with no runtime policy check (detection confidence: medium).
                #   Decision: the Guardrail evaluates AI_DAT_SEC_029; sensitive values in `compiled_code` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:b2dbfb65cd4743677ba2f7e75063641b8fbd9e30ec4818817a70953b600c5af5'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:b2dbfb65cd4743677ba2f7e75063641b8fbd9e30ec4818817a70953b600c5af5', phase='security_decision', boundary={'source': 'agent_message', 'sink': 'agent_message'}, fail_mode='BLOCK', source_type='agent', destination_type='system', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 233, 'function': 'TaskExecutor._per_item'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 233, 'function': '_per_item', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
                try:
                    compiled_code = _gr_client.enforce(_gr_site, compiled_code, content_type='application/json', variable_name='compiled_code', source_file=__file__, before_line=233)
                except _gr_client.GuardrailUnavailableError:
                    pass
                exec(compiled_code, globals)

                user_output = globals[EXECUTOR_USER_OUTPUT_KEY]

                if user_output is None:
                    continue

                json_data = TaskExecutor._extract_json_data_per_item(user_output)

                item = {"json": json_data, "pairedItem": {"item": index}}

                if isinstance(user_output, dict) and "binary" in user_output:
                    item["binary"] = user_output["binary"]

                result.append(item)

            TaskExecutor._put_result(queue, result, print_args)

        except BaseException as e:
            TaskExecutor._put_error(queue, e, stderr_capture.getvalue(), print_args)

    @staticmethod
    def _wrap_code(raw_code: str) -> str:
        indented_code = textwrap.indent(raw_code, "    ")
        return f"def _user_function():\n{indented_code}\n\n{EXECUTOR_USER_OUTPUT_KEY} = _user_function()"

    @staticmethod
    def _extract_json_data_per_item(user_output):
        if not isinstance(user_output, dict):
            # LINEAJE: runtime guardrail checks `user_output` as it flows from the agent to the user interface, before it leaves the application (in TaskExecutor._extract_json_data_per_item).
            #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `user_output` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:d85cb70e009d30cc243b43cb063facbc06eef0bb2be48278c106676c7483819e'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:d85cb70e009d30cc243b43cb063facbc06eef0bb2be48278c106676c7483819e', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 262, 'function': 'TaskExecutor._extract_json_data_per_item'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 262, 'function': '_extract_json_data_per_item', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                user_output = _gr_client.enforce(_gr_site, user_output, content_type='text/plain')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            return user_output

        if "json" in user_output:
            return user_output["json"]

        if "binary" in user_output:
            return {k: v for k, v in user_output.items() if k != "binary"}

        # LINEAJE: runtime guardrail checks `user_output` as it flows from the agent to the user interface, before it leaves the application (in TaskExecutor._extract_json_data_per_item).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `user_output` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:68f1721aa8e406c19d8e92d36138c092e96725559fa734676f598ec79cce3816'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:68f1721aa8e406c19d8e92d36138c092e96725559fa734676f598ec79cce3816', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 270, 'function': 'TaskExecutor._extract_json_data_per_item'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 270, 'function': '_extract_json_data_per_item', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            user_output = _gr_client.enforce(_gr_site, user_output, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return user_output

    @staticmethod
    def _put_result(
        queue: multiprocessing.Queue, result: list[Any], print_args: PrintArgs
    ):
        # LINEAJE: runtime guardrail checks `result` as it flows from the agent to an external endpoint, before it leaves the application (in TaskExecutor._put_result).
        #   Detected: this statement moves data from the agent to an external endpoint with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates the policies enabled for this boundary; sensitive values in `result` are masked in place; a policy violation is logged and the call continues (fail_mode=ALLOW_WITH_AUDIT). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:fd426040e5f371d138b51d89fa4a5a196b8e484520ed1a5149f88136fd6d0076'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:fd426040e5f371d138b51d89fa4a5a196b8e484520ed1a5149f88136fd6d0076', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'external_endpoint'}, fail_mode='ALLOW_WITH_AUDIT', source_type='agent', destination_type='external', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 276, 'function': 'TaskExecutor._put_result'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 276, 'function': '_put_result', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            result = _gr_client.enforce(_gr_site, result, content_type='application/json', variable_name='result', source_file=__file__, before_line=276)
        except _gr_client.GuardrailUnavailableError:
            pass
        json_bytes = json.dumps(result, default=str, ensure_ascii=False).encode("utf-8")
        json_bytes_size = len(json_bytes)

        shm = shared_memory.SharedMemory(create=True, size=json_bytes_size)
        shm.buf[:json_bytes_size] = json_bytes

        print_args_to_send = TaskExecutor._truncate_print_args(print_args)
        queue.put(
            {
                "shm_name": shm.name,
                "shm_size": json_bytes_size,  # stay exact, shm.size can round up for alignment
                "print_args": print_args_to_send,
            }
        )

        shm.close()

    @staticmethod
    def _put_error(
        queue: multiprocessing.Queue,
        e: BaseException,
        stderr: str = "",
        print_args: PrintArgs = [],
    ):
        error_dict = {
            "message": f"Process exited with code {e.code}"
            if isinstance(e, SystemExit)
            else str(e),
            "description": getattr(e, "description", ""),
            "stack": traceback.format_exc(),
            "stderr": stderr,
        }

        print_args_to_send = TaskExecutor._truncate_print_args(print_args)

        queue.put(
            {
                "error": error_dict,
                "print_args": print_args_to_send,
            }
        )

    # ========== print() ==========

    @staticmethod
    def _create_custom_print(print_args: PrintArgs):
        def custom_print(*args):
            serializable_args = []

            for arg in args:
                try:
                    # LINEAJE: runtime guardrail checks `arg` as it flows from the agent to an external endpoint, before it leaves the application (in TaskExecutor.custom_print).
                    #   Detected: this statement moves data from the agent to an external endpoint with no runtime policy check (detection confidence: high).
                    #   Decision: the Guardrail evaluates the policies enabled for this boundary; sensitive values in `arg` are masked in place; a policy violation is logged and the call continues (fail_mode=ALLOW_WITH_AUDIT). If the Guardrail is unreachable, the call continues unchanged.
                    #   Do not remove without review. site_id='site:sha256:10b9b0515b9425b00dc5a2e5140f75d9086f940b2e1778aca303e03b9feb1890'
                    _gr_client = _lineaje_load_gr_client()
                    _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:10b9b0515b9425b00dc5a2e5140f75d9086f940b2e1778aca303e03b9feb1890', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'external_endpoint'}, fail_mode='ALLOW_WITH_AUDIT', source_type='agent', destination_type='external', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 327, 'function': 'TaskExecutor._create_custom_print.custom_print'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 327, 'function': 'custom_print', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
                    try:
                        arg = _gr_client.enforce(_gr_site, arg, content_type='application/json', variable_name='arg', source_file=__file__, before_line=327)
                    except _gr_client.GuardrailUnavailableError:
                        pass
                    json.dumps(arg, default=str, ensure_ascii=False)
                    serializable_args.append(arg)
                except Exception as _:
                    # Ensure args are serializable so they are transmissible
                    # through the multiprocessing queue and via websockets.
                    serializable_args.append(
                        {
                            EXECUTOR_CIRCULAR_REFERENCE_KEY: repr(arg),
                            "__type__": type(arg).__name__,
                        }
                    )

            formatted = TaskExecutor._format_print_args(*serializable_args)
            print_args.append(formatted)
            _lineaje_payload = "[user code]"
            # LINEAJE: runtime guardrail checks `_lineaje_payload` as it flows from the agent to the application log, before it is written to the log (in TaskExecutor.custom_print).
            #   Detected: this statement moves data from the agent to the application log with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_010; sensitive values in `_lineaje_payload` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:88d186ae4cc9469d26740b55836df1ba5571f18690c48f1c4e88250003724969'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:88d186ae4cc9469d26740b55836df1ba5571f18690c48f1c4e88250003724969', phase='log_emit', boundary={'source': 'log', 'sink': 'log'}, fail_mode='BLOCK', source_type='agent', destination_type='log', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 341, 'function': 'TaskExecutor._create_custom_print.custom_print'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 341, 'function': 'custom_print', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                _lineaje_payload = _gr_client.enforce(_gr_site, _lineaje_payload, content_type='application/json')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            print(_lineaje_payload, *args)

        # LINEAJE: runtime guardrail checks `custom_print` as it flows from the agent to the user interface, before it leaves the application (in TaskExecutor._create_custom_print).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `custom_print` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:251f2502fbfe6714dee0dde5c1033340f4ed35777277a40761615d77879659ef'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:251f2502fbfe6714dee0dde5c1033340f4ed35777277a40761615d77879659ef', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 343, 'function': 'TaskExecutor._create_custom_print'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 343, 'function': '_create_custom_print', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            custom_print = _gr_client.enforce(_gr_site, custom_print, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return custom_print

    @staticmethod
    def _format_print_args(*args) -> list[str]:
        """
        Takes the args passed to a `print()` call in user code and converts them
        to string representations suitable for display in a browser console.

        Expects all args to be serializable.
        """

        formatted = []

        for arg in args:
            if isinstance(arg, str):
                formatted.append(f"'{arg}'")

            elif arg is None or isinstance(arg, (int, float, bool)):
                formatted.append(str(arg))

            elif isinstance(arg, dict) and EXECUTOR_CIRCULAR_REFERENCE_KEY in arg:
                formatted.append(f"[Circular {arg.get('__type__', 'Object')}]")

            else:
                # LINEAJE: runtime guardrail checks `arg` as it flows from the agent to an external endpoint, before it leaves the application (in TaskExecutor._format_print_args).
                #   Detected: this statement moves data from the agent to an external endpoint with no runtime policy check (detection confidence: high).
                #   Decision: the Guardrail evaluates the policies enabled for this boundary; sensitive values in `arg` are masked in place; a policy violation is logged and the call continues (fail_mode=ALLOW_WITH_AUDIT). If the Guardrail is unreachable, the call continues unchanged.
                #   Do not remove without review. site_id='site:sha256:71242d534b8daa2f274c3d0758bdf9b7065a2ec01ef6705dbf98e84423ed2b35'
                _gr_client = _lineaje_load_gr_client()
                _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:71242d534b8daa2f274c3d0758bdf9b7065a2ec01ef6705dbf98e84423ed2b35', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'external_endpoint'}, fail_mode='ALLOW_WITH_AUDIT', source_type='agent', destination_type='external', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 367, 'function': 'TaskExecutor._format_print_args'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 367, 'function': '_format_print_args', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
                try:
                    arg = _gr_client.enforce(_gr_site, arg, content_type='application/json', variable_name='arg', source_file=__file__, before_line=367)
                except _gr_client.GuardrailUnavailableError:
                    pass
                formatted.append(json.dumps(arg, default=str, ensure_ascii=False))

        # LINEAJE: runtime guardrail checks `formatted` as it flows from the agent to the user interface, before it leaves the application (in TaskExecutor._format_print_args).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `formatted` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:9d83bfa7ce7198b74e0a5a13a9b943a73af3415e108687c25d14bbefb4598a75'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:9d83bfa7ce7198b74e0a5a13a9b943a73af3415e108687c25d14bbefb4598a75', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 369, 'function': 'TaskExecutor._format_print_args'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 369, 'function': '_format_print_args', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            formatted = _gr_client.enforce(_gr_site, formatted, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return formatted

    @staticmethod
    def _truncate_print_args(print_args: PrintArgs) -> PrintArgs:
        """Truncate print_args to prevent pipe buffer overflow."""

        if not print_args or len(print_args) <= MAX_PRINT_ARGS_ALLOWED:
            # LINEAJE: runtime guardrail checks `print_args` as it flows from the agent to the user interface, before it leaves the application (in TaskExecutor._truncate_print_args).
            #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
            #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `print_args` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
            #   Do not remove without review. site_id='site:sha256:1e783b0fdcca75ef1b104240045f69d47e7a866a7422460ae9dbcf960308047b'
            _gr_client = _lineaje_load_gr_client()
            _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:1e783b0fdcca75ef1b104240045f69d47e7a866a7422460ae9dbcf960308047b', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 376, 'function': 'TaskExecutor._truncate_print_args'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 376, 'function': '_truncate_print_args', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
            try:
                print_args = _gr_client.enforce(_gr_site, print_args, content_type='text/plain')
            except _gr_client.GuardrailUnavailableError:
                pass
            except PermissionError:
                pass
            return print_args

        truncated = print_args[:MAX_PRINT_ARGS_ALLOWED]
        truncated.append(
            [
                f"[Output truncated - {len(print_args) - MAX_PRINT_ARGS_ALLOWED} more print statements]"
            ]
        )

        # LINEAJE: runtime guardrail checks `truncated` as it flows from the agent to the user interface, before it leaves the application (in TaskExecutor._truncate_print_args).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `truncated` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:575a0a6a0de6b62a5c11b903ddc0d138b62225e1e3e68db40ef241d36b35e665'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:575a0a6a0de6b62a5c11b903ddc0d138b62225e1e3e68db40ef241d36b35e665', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 385, 'function': 'TaskExecutor._truncate_print_args'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 385, 'function': '_truncate_print_args', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            truncated = _gr_client.enforce(_gr_site, truncated, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return truncated

    # ========== security ==========

    @staticmethod
    def _filter_builtins(security_config: SecurityConfig):
        """Get __builtins__ with denied ones removed."""

        if len(security_config.builtins_deny) == 0:
            filtered = dict(__builtins__)
        else:
            filtered = {
                k: v
                for k, v in __builtins__.items()
                if k not in security_config.builtins_deny
            }

        filtered["__import__"] = TaskExecutor._create_safe_import(security_config)

        # LINEAJE: runtime guardrail checks `filtered` as it flows from the agent to the user interface, before it leaves the application (in TaskExecutor._filter_builtins).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `filtered` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:e28247afcaf2d3595d734f72898e12915a43943cba4d9d718499965da30eb458'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:e28247afcaf2d3595d734f72898e12915a43943cba4d9d718499965da30eb458', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 404, 'function': 'TaskExecutor._filter_builtins'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 404, 'function': '_filter_builtins', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            filtered = _gr_client.enforce(_gr_site, filtered, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return filtered

    @staticmethod
    def _sanitize_sys_modules(security_config: SecurityConfig):
        safe_modules = {
            "builtins",
            "__main__",
            "sys",
            "traceback",
            "linecache",
            "importlib",
            "importlib.machinery",
        }

        if "*" in security_config.stdlib_allow:
            safe_modules.update(sys.stdlib_module_names)
        else:
            safe_modules.update(security_config.stdlib_allow)

        if "*" in security_config.external_allow:
            safe_modules.update(
                name
                for name in sys.modules.keys()
                if name not in sys.stdlib_module_names
            )
        else:
            safe_modules.update(security_config.external_allow)

        # keep modules marked as safe and submodules of those
        safe_prefixes = [safe + "." for safe in safe_modules]
        modules_to_remove = [
            name
            for name in sys.modules.keys()
            if name not in safe_modules
            and not any(name.startswith(prefix) for prefix in safe_prefixes)
        ]

        for module_name in modules_to_remove:
            del sys.modules[module_name]

    @staticmethod
    def _create_safe_import(security_config: SecurityConfig):
        original_import = __builtins__["__import__"]

        def safe_import(name, *args, **kwargs):
            is_allowed, error_msg = validate_module_import(name, security_config)

            if not is_allowed:
                assert error_msg is not None
                raise SecurityViolationError(
                    message="Security violation detected",
                    description=error_msg,
                )

            return original_import(name, *args, **kwargs)

        # LINEAJE: runtime guardrail checks `safe_import` as it flows from the agent to the user interface, before it leaves the application (in TaskExecutor._create_safe_import).
        #   Detected: this statement moves data from the agent to the user interface with no runtime policy check (detection confidence: high).
        #   Decision: the Guardrail evaluates AI_DAT_SEC_012; sensitive values in `safe_import` are masked in place; a policy violation stops the call (fail_mode=BLOCK). If the Guardrail is unreachable, the call continues unchanged.
        #   Do not remove without review. site_id='site:sha256:fc58f099b707d0f68b336732a8e0eee5d1de589bd5c7fe8f1c5befd57e7efecb'
        _gr_client = _lineaje_load_gr_client()
        _gr_site = _gr_client.SiteDescriptor(site_id='site:sha256:fc58f099b707d0f68b336732a8e0eee5d1de589bd5c7fe8f1c5befd57e7efecb', phase='data_egress', boundary={'source': 'agent_message', 'sink': 'user_interface'}, fail_mode='BLOCK', source_type='agent', destination_type='user_interface', components={'invocation': {'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 460, 'function': 'TaskExecutor._create_safe_import'}, 'file': 'packages/@n8n/task-runner-python/src/task_executor.py', 'line': 460, 'function': '_create_safe_import', 'agent_name': 'TaskExecutor', 'agent_name_source': 'inferred_from_class', 'not_on_graph': True})
        try:
            safe_import = _gr_client.enforce(_gr_site, safe_import, content_type='text/plain')
        except _gr_client.GuardrailUnavailableError:
            pass
        except PermissionError:
            pass
        return safe_import
