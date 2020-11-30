# Changelog

## [v0.3.4]

- Add summary for long listing, use human readable sizes
- Add several aliases for list operations (e.g. `bbb ll`)
- Fixed syncing in the presence of directory marker files
- Only log retrying of requests after a couple attempts
- Release responses before yielding in paginated requests
- Fixed Azure auth edge case with service principals
- Documentation improvements

## [v0.3.3]

- Speed up `bbb sync`

## [v0.3.2]

- Preliminary support for wildcards in `bbb ls` and `bbb rm`
- Fix `bbb ls -l <file>` to show info
- Use a per token expiration within each TokenManager

## [v0.3.1]

- Utilise cached subscriptions; speeds up storage key based Azure auth
- Fixed `isdir` for an Azure edge case
- Lower default retry limits
- Better error message for non-existent Azure storage account

## [v0.3.0]

- Support specifying Azure paths as `az://storage_account/container/blob`
- Better error when listing a storage account with no containers
- Set BBB_TRACEBACK env variable to get a traceback on error

## [v0.2.2]

- `bbb ls <file>` now works; it no longer complains about the file not being a directory
- More helpful error message for token refresh failures

## [v0.2.1]

- Fixed shared key request signing
- Adjusted dependency specifications

## [v0.2.0]

- Added `bbb sync`
- Fix for listing directory marker files
- Faster pip installs

## [v0.1.4]

- Improved `--help` output for subcommands
- `bbb ls gs://` will now attempt to list all buckets
- Added short aliases for lstree, cptree, rmtree
- Added this changelog!

## [v0.1.3]

For changes before this, see git :-)
