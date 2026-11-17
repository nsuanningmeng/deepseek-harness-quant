"""Reversible macOS login services for the local, guarded paper workspace."""
import argparse
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
import yaml
import psutil

from autotrade.locking import ProcessLock

ROOT = Path(__file__).resolve().parent.parent


def load_policy(base=ROOT):
    path = base / 'config/local_services.yaml'
    if not path.exists():
        path = base / 'config/local_services.yaml.example'
    policy = yaml.safe_load(path.read_text())
    if not re.fullmatch(r'[A-Za-z0-9.-]+', policy['label_prefix']):
        raise ValueError('Invalid local service label')
    if not 10 <= policy['restart_delay_seconds'] <= 300:
        raise ValueError('Invalid local restart delay')
    for name, service in policy['services'].items():
        if not re.fullmatch(r'[a-z][a-z0-9_]*', name) or type(service['enabled']) is not bool:
            raise ValueError('Invalid local service configuration')
        if not service['command'] or not all(isinstance(part, str) for part in service['command']):
            raise ValueError('Commands must be argument lists, never shell strings')
    return policy


def resolve_service(name, policy=None, base=ROOT, *, validate_executable=True):
    policy = policy or load_policy(base)
    service = dict(policy['services'][name])
    minute_module = ''
    if service.get('pid_file_from_minute_config'):
        from data.minute_readiness import selected_service
        module = selected_service()
        minute_module = module.__name__
        minute_root = Path(module.load_config()['runtime_dir'])
        service.update(pid_file=minute_root / 'service.pid', log_file=minute_root / 'service.log')
    if service.get('pid_file_from_paper_config'):
        from paperlive.config import load_config
        paper_root = Path(load_config(base / 'config/paperlive.yaml')['runtime_dir'])
        service.update(pid_file=paper_root / 'runner.pid', log_file=paper_root / 'runner.log')
    substitutions = {'root': str(base.resolve()), 'python': str(base / '.venv/bin/python'),
                     'node': shutil.which('node', path=policy['path']) or '', 'minute_module': minute_module}
    service['command'] = [part.format(**substitutions) for part in service['command']]
    if validate_executable and (not service['command'][0] or not Path(service['command'][0]).is_file()):
        raise ValueError('Configured service executable is unavailable')
    service['environment'] = {key: str(value).format(**substitutions)
                              for key, value in service.get('environment', {}).items()}
    for key in ('pid_file', 'log_file'):
        path = Path(service[key])
        path = (path if path.is_absolute() else base / path).resolve()
        if not path.is_relative_to(base.resolve() / 'runtime'):
            raise ValueError('Service state must stay inside this workspace runtime')
        service[key] = path
    service['label'] = policy['label_prefix'] + '.' + name
    return service


def running_service(service, base=ROOT):
    """Find only this workspace's exact service; never infer ownership from a port/PID."""
    command = service['command']
    if not command or not command[0]:
        return None
    executables = {Path(command[0]).resolve()}
    # macOS framework Python execs its app binary while retaining the same
    # interpreter. Resolve that observed identity, without accepting arbitrary
    # Python processes or relying on their display names.
    if Path(command[0]).resolve() == Path(sys.executable).resolve():
        executables.add(Path(psutil.Process(os.getpid()).exe()).resolve())
    for process in psutil.process_iter(['pid', 'cmdline', 'cwd', 'exe', 'status']):
        try:
            info = process.info
            actual = info.get('cmdline') or []
            if (info.get('status') != psutil.STATUS_ZOMBIE and actual and info.get('exe')
                    and actual[1:] == command[1:]
                    and Path(info.get('cwd') or '/').resolve() == base.resolve()
                    and Path(info['exe']).resolve() in executables
                    and process.is_running()):
                return process
        except (OSError, psutil.Error):
            continue
    return None


def entry_lock(service):
    return ProcessLock(service['pid_file'].with_name(service['pid_file'].name + '.entry.lock'))


def publish_pid(service, pid):
    service['pid_file'].parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='runner-', suffix='.pid.new', dir=service['pid_file'].parent)
    try:
        with os.fdopen(fd, 'w') as handle:
            handle.write(str(pid) + '\n')
        Path(name).replace(service['pid_file'])
    finally:
        Path(name).unlink(missing_ok=True)


def launch_definition(name, policy=None, base=ROOT):
    policy = policy or load_policy(base)
    service = resolve_service(name, policy, base)
    # launchd itself cannot open protected Documents paths. Keep its startup
    # diagnostics in the standard user Logs directory; the service opens its
    # own project log under its existing filesystem permissions.
    bootstrap_dir = Path(policy.get('bootstrap_log_directory', str(base / 'runtime/login'))).expanduser()
    if not bootstrap_dir.is_absolute():
        raise ValueError('Startup log directory must be absolute')
    bootstrap_log = bootstrap_dir / (service['label'] + '.log')
    return {'Label': service['label'],
            'ProgramArguments': [str(base / '.venv/bin/python'), '-u', '-m', 'data.local_services', 'run', '--service', name],
            'WorkingDirectory': str(base.resolve()),
            'EnvironmentVariables': {'PATH': policy['path'], 'PYTHONUNBUFFERED': '1'},
            'RunAtLoad': True, 'KeepAlive': {'SuccessfulExit': False},
            'ThrottleInterval': policy['restart_delay_seconds'], 'ProcessType': 'Background',
            'StandardOutPath': str(bootstrap_log), 'StandardErrorPath': str(bootstrap_log),
            'Umask': 0o077}


def redirect_runtime_logs(service):
    service['log_file'].parent.mkdir(parents=True, exist_ok=True)
    with service['log_file'].open('ab', buffering=0) as handle:
        os.dup2(handle.fileno(), 1)
        os.dup2(handle.fileno(), 2)


def run_service(name):
    policy = load_policy()
    service = resolve_service(name, policy, validate_executable=policy['services'][name]['enabled'])
    if not service['enabled']:
        return
    os.chdir(ROOT)
    os.umask(0o077)
    with entry_lock(service) as lock:
        if running_service(service, ROOT) is not None:
            raise RuntimeError('This workspace service is already running; preserving its PID and state')
        # Keep the entry lock across exec so another launcher cannot race the
        # interval before the service obtains its own lock or listening port.
        os.set_inheritable(lock.handle.fileno(), True)
        if os.environ.get('XPC_SERVICE_NAME') == service['label']:
            redirect_runtime_logs(service)
        publish_pid(service, os.getpid())
        environment = {**os.environ, 'PATH': policy['path'], **service['environment']}
        os.execve(service['command'][0], service['command'], environment)


def check_ownership(path, name, base=ROOT):
    if path.is_symlink():
        raise ValueError('Refusing to replace a linked login-service file')
    if path.exists():
        existing = plistlib.loads(path.read_bytes())
        arguments = existing.get('ProgramArguments', [])
        if (existing.get('WorkingDirectory') != str(base.resolve())
                or 'data.local_services' not in arguments or arguments[-2:] != ['--service', name]):
            raise ValueError('An existing login service belongs to another workspace')


def manage(action, only=None):
    if sys.platform != 'darwin':
        raise ValueError('Login-service installation is only supported on macOS')
    policy = load_policy()
    if only is not None and only not in policy['services']:
        raise ValueError('Unknown local service')
    directory = Path.home() / 'Library/LaunchAgents'
    domain = f'gui/{os.getuid()}'
    directory.mkdir(parents=True, exist_ok=True)
    result = []
    for name in ([only] if only is not None else policy['services']):
        service = resolve_service(name, policy,
                                  validate_executable=action == 'install' and policy['services'][name]['enabled'])
        path = directory / (service['label'] + '.plist')
        target = domain + '/' + service['label']
        loaded = subprocess.run(['launchctl', 'print', target], capture_output=True).returncode == 0
        if action == 'status':
            result.append({'service': name, 'loaded': loaded, 'plist': str(path)})
            continue
        check_ownership(path, name)
        if loaded and not path.exists():
            raise ValueError('An existing service has no verifiable local ownership file')
        definition = launch_definition(name, policy) if action == 'install' and service['enabled'] else None
        if loaded and definition is not None and plistlib.loads(path.read_bytes()) == definition:
            # A normal code reload does not need service deregistration. macOS
            # may finish bootout asynchronously, rejecting immediate bootstrap.
            subprocess.run(['launchctl', 'kickstart', '-k', target], check=True, capture_output=True)
            result.append({'service': name, 'installed': True, 'plist': str(path), 'restarted': True})
            continue
        if loaded:
            subprocess.run(['launchctl', 'bootout', target], check=True, capture_output=True)
        if action == 'remove' or not service['enabled']:
            path.unlink(missing_ok=True)
            result.append({'service': name, 'installed': False})
            continue
        service['log_file'].parent.mkdir(parents=True, exist_ok=True)
        startup_log = Path(definition['StandardOutPath'])
        startup_log.parent.mkdir(parents=True, exist_ok=True)
        if startup_log.is_symlink():
            raise ValueError('Refusing to follow a linked startup log')
        startup_log.touch(mode=0o600, exist_ok=True)
        startup_log.chmod(0o600)
        path.write_bytes(plistlib.dumps(definition))
        path.chmod(0o600)
        for attempt in range(5 if loaded else 1):
            try:
                subprocess.run(['launchctl', 'bootstrap', domain, str(path)], check=True, capture_output=True)
                break
            except subprocess.CalledProcessError as exc:
                if exc.returncode != 5 or attempt == 4 or not loaded:
                    raise
                time.sleep(0.25 * 2 ** attempt)
        result.append({'service': name, 'installed': True, 'plist': str(path)})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['run', 'install', 'remove', 'status'])
    parser.add_argument('--service')
    args = parser.parse_args()
    if args.action == 'run':
        if not args.service:
            parser.error('--service is required for run')
        run_service(args.service)
    else:
        print(json.dumps(manage(args.action, only=args.service), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
