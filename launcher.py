# -*- coding: utf-8 -*-
"""QuantDeck 启动器：一键启动 量化 Deck（:8787）+ DeepSeek HARNESS（:3080）。

行为：
  - 数据目录：环境变量 LWQUANT_CACHE_DIR > EXE/仓库 同级 data/
  - HARNESS：若存在 harness/ 运行时且系统有 Node.js → 自动启动（DSH_HOME=harness/home）
    · 首次使用：把 harness/home/.credentials.yaml.example 复制为 .credentials.yaml
      并填入 DeepSeek API Key（先接 AI 的 API，AI 会协助你接入其余数据源 API）
    · 没有 Node.js 时跳过 HARNESS（量化系统照常运行）
  - 浏览器：自动打开 量化门户 http://127.0.0.1:8787（HARNESS GUI 为 3080，可手动打开）
"""
import os
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path


def base_dir() -> Path:
    if getattr(sys, "frozen", False):          # PyInstaller 打包后
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent     # 源码运行：仓库根


def _builtin_python():
    """优先使用发布包内置便携 Python（runtime/python 或 portable-py）"""
    for c in (base_dir() / "runtime" / "python" / "python.exe",
              base_dir() / "portable-py" / "Scripts" / "python.exe"):
        if c.exists():
            return c
    return None


def _builtin_node():
    """优先使用发布包内置便携 Node（runtime/node 或 portable-node）"""
    for c in (base_dir() / "runtime" / "node" / "node.exe",
              base_dir() / "portable-node" / "node.exe"):
        if c.exists():
            return c
    return None


def start_harness(base: Path):
    """启动 DeepSeek HARNESS（harness/ 存在且 Node.js 可用时）。返回进程或 None。"""
    node = _builtin_node() or shutil.which("node")
    harness_root = base / "harness"
    dsh_bin = harness_root / "node_modules" / "@deepseek-ai" / "dsh" / "lib" / "bin.js"
    if not node:
        print("[QuantDeck] 未检测到 Node.js —— HARNESS 对话功能跳过（量化系统照常）")
        return None
    if not dsh_bin.exists():
        print("[QuantDeck] harness 运行时缺失 —— 请先运行 harness/install.cmd 安装 HARNESS")
        return None
    cred = harness_root / "home" / ".credentials.yaml"
    if not cred.exists():
        print("[QuantDeck] HARNESS 未配置 API Key：把 harness/home/.credentials.yaml.example "
              "复制为 .credentials.yaml 并填入 DeepSeek API Key（见 docs/HARNESS接入.md）")
    env = dict(os.environ)
    env["DSH_HOME"] = str(harness_root / "home")
    env["DSH_TELEMETRY_DISABLED"] = "1"
    print(f"[QuantDeck] 启动 DeepSeek HARNESS（DSH_HOME={env['DSH_HOME']}）...")
    try:
        from data.local_services import entry_lock, load_policy, publish_pid, resolve_service, running_service
        log_dir = base / "runtime" / "ai"
        log_dir.mkdir(parents=True, exist_ok=True)
        service = {'command': [str(node), str(dsh_bin), 'web'],
                   'pid_file': log_dir / 'harness.pid', 'log_file': log_dir / 'harness.log'}
        # macOS login recovery and the manual launcher must use the same
        # command. Other platforms retain their portable Node selection.
        if sys.platform == 'darwin' and (base / 'config/local_services.yaml').exists():
            policy = load_policy(base)
            service = resolve_service('ai', policy, base)
            if not service['enabled']:
                return None
            env.update({'PATH': policy['path'], **service['environment']})
        existing = running_service(service, base)
        if existing is not None:
            print('[QuantDeck] HARNESS 已在运行')
            return existing
        with entry_lock(service):
            existing = running_service(service, base)
            if existing is not None:
                print('[QuantDeck] HARNESS 已在运行')
                return existing
            service['log_file'].parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(service['log_file'], os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            with os.fdopen(fd, "ab") as log:
                proc = subprocess.Popen(service['command'], cwd=str(base), env=env,
                                        stdout=log, stderr=log)
            publish_pid(service, proc.pid)
            return proc
    except Exception as e:
        print(f"[QuantDeck] HARNESS 启动失败: {e}")
        return None


def _open_browsers():
    time.sleep(2.0)
    try:
        webbrowser.open("http://127.0.0.1:8787")
    except Exception:
        pass


def start_paperlive(base: Path):
    """Resume the separately configured quote-driven virtual account."""
    if not (base / "config" / "paperlive.yaml").exists():
        return None
    from paperlive.config import load_config
    configuration = base / 'config/paperlive.yaml'
    cfg = load_config(configuration)
    if Path(cfg['risk']['kill_switch_file']).exists() and not cfg.get('activation', {}).get('enabled'):
        print('[QuantDeck] 盘中模拟已暂停，保留原虚拟账户')
        return None
    log_dir = Path(cfg['runtime_dir'])
    log_dir.mkdir(parents=True, exist_ok=True)
    pid_file = log_dir / "runner.pid"
    try:
        import psutil
        proc = psutil.Process(int(pid_file.read_text()))
        if "paperlive" in proc.cmdline() and Path(proc.cwd()).resolve() == base.resolve():
            print("[QuantDeck] 实时模拟账户已在运行")
            return proc
    except Exception:
        pass
    with (log_dir / "runner.log").open("ab") as log:
        proc = subprocess.Popen([sys.executable, "-u", "-m", "paperlive", "--config", str(configuration), "run"],
                                cwd=str(base), stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    pid_file.write_text(str(proc.pid) + "\n", encoding="utf-8")
    print("[QuantDeck] 行情验收后台已启动；是否模拟以条件验收和暂停状态为准：/paperlive")
    return proc


def start_research(base: Path):
    """Start the independent, verified after-close research updater."""
    if not (base / 'config/research_service.yaml').exists():
        return None
    try:
        from data.research_service import start_service
        proc=start_service(base)
        print('[QuantDeck] 研究更新服务已启用：官方收盘数据核实后更新')
        return proc
    except Exception:
        print('[QuantDeck] 研究更新服务启动失败，请检查本机研究服务配置与日志')
        return None


def start_research_minute(base: Path):
    """Start the selected, isolated public-minute cache maintainer."""
    if not (base / 'config/research_minute_source.yaml').exists():
        return None
    try:
        from data.minute_readiness import start_service
        proc = start_service(base)
        print('[QuantDeck] 分钟研究数据后台已启用；完整性以真实数据验收为准')
        return proc
    except Exception:
        print('[QuantDeck] 分钟研究数据后台启动失败，请检查独立日志')
        return None


def main():
    base = base_dir()
    if not os.environ.get("LWQUANT_CACHE_DIR"):
        from data.cache import CACHE_DIR
        os.environ["LWQUANT_CACHE_DIR"] = str(CACHE_DIR)
    data_dir = os.environ["LWQUANT_CACHE_DIR"]
    print(f"[QuantDeck] 数据目录: {data_dir}")
    if not (Path(data_dir) / "bars.db").exists():
        print("[QuantDeck] 提示: 未检测到 bars.db —— 请先获取数据（docs/快速开始.md §3），或使用演示数据模式")

    start_harness(base)                 # HARNESS（可选，:3080）
    start_paperlive(base)               # 当前市场报价 + 独立虚拟账户
    start_research(base)                # 官方盘后数据 + 独立研究产物
    start_research_minute(base)         # 独立分钟研究库，不操作账户
    threading.Thread(target=_open_browsers, daemon=True).start()

    from deck.deck_server import Handler, PORT, ThreadingHTTPServer

    print(f"[QuantDeck] 量化系统已启动: http://127.0.0.1:{PORT}  （Ctrl+C 退出）")
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
