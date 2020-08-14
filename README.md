# boostedblob

boostedblob is a command line tool and async library to perform basic file operations on local
paths, Google Cloud Storage paths and Azure Blob Storage paths.

boostedblob is derived from the excellent [blobfile](https://github.com/christopher-hesse/blobfile).

The fun part of implementing boostedblob is `boostedblob/boost.py`, which provides a
`concurrent.futures`-like interface for running and composing async tasks in a concurrency limited
environment.