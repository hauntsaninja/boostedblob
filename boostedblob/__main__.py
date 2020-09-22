import sys

from boostedblob.cli import run_bbb

if __name__ == "__main__":
    try:
        run_bbb(sys.argv[1:])
    except Exception:
        sys.exit(1)
