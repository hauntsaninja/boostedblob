# boostedblob

boostedblob is a command line tool and async library to perform basic file operations on local
paths, Google Cloud Storage paths and Azure Blob Storage paths.

boostedblob is derived from the excellent [blobfile](https://github.com/christopher-hesse/blobfile).

The fun part of implementing boostedblob is `boostedblob/boost.py`, which provides a
`concurrent.futures`-like interface for running and composing async tasks in a concurrency limited
environment.

## Installation

Just run `pip install boostedblob`. boostedblob requires Python 3.8 or better.

## Usage

For an overview and list of commands:
```sh
bbb --help
```

For help with a specific command:
```sh
bbb ls --help
```

To enable tab completion, add the following to your shell config (replacing `zsh` with `bash`,
if appropriate):
```sh
eval "$(bbb complete init zsh)"
```
Note that the quotes are necessary. You can also inline the result of `bbb complete init zsh` into
your shell config to make your shell startup a little faster.

## Contributing

For developer documentation (getting started, running tests, debugging tricks, codebase tips),
see [CONTRIBUTING.md](CONTRIBUTING.md)
