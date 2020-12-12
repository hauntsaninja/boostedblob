import os
import sys

from boostedblob.cli import run_bbb


def main() -> None:
    try:
        run_bbb(sys.argv[1:])
    except Exception:
        if os.environ.get("BBB_DEBUG") or os.environ.get("BBB_TRACEBACK"):
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
