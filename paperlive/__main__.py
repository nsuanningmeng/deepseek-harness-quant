import argparse
import json
from .config import load_config


def main():
    parser = argparse.ArgumentParser(description='Live public quotes and virtual paper capital only')
    parser.add_argument('--config')
    parser.add_argument('command', choices=['run', 'once', 'status'])
    args = parser.parse_args()
    from .runtime import run, read_state
    cfg = load_config(args.config)
    result = read_state(cfg) if args.command == 'status' else run(cfg, once=args.command == 'once')
    if result is not None:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
