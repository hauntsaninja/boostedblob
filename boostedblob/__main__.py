import os
import signal
import sys

from boostedblob.cli import run_bbb

BROKEN_PIPE_EXIT_CODE = 128 + int(getattr(signal, "SIGPIPE", 13))


def silence_broken_pipe() -> None:
    try:
        stdout_fd = sys.stdout.fileno()
    except (OSError, ValueError):
        return

    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        try:
            # Avoid another BrokenPipeError when Python flushes stdout at shutdown.
            os.dup2(devnull, stdout_fd)
        finally:
            os.close(devnull)
    except OSError:
        pass


def main() -> None:
    try:
        run_bbb(sys.argv[1:])
    except BrokenPipeError:
        silence_broken_pipe()
        sys.exit(BROKEN_PIPE_EXIT_CODE)
    except Exception:
        if os.environ.get("BBB_DEBUG") or os.environ.get("BBB_TRACEBACK"):
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
