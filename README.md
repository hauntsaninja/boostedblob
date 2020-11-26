# boostedblob

boostedblob is a command line tool and async library to perform basic file operations on local
paths, Google Cloud Storage paths and Azure Blob Storage paths.

boostedblob is derived from the excellent [blobfile](https://github.com/christopher-hesse/blobfile).

The fun part of implementing boostedblob is `boostedblob/boost.py`, which provides a
`concurrent.futures`-like interface for running and composing async tasks in a concurrency limited
environment.

## Installation

Just run `pip install boostedblob`. boostedblob requires Python 3.7 or better.

<sup>(For Python 3.9 support, you'll need to run `pip uninstall uvloop`)</sup>


## Usage

For an overview and list of commands:
```sh
bbb --help
```

For help with a specific command:
```sh
bbb ls --help
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
