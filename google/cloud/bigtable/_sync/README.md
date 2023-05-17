# Bigtable Sync Surface Maintainer Guide

To reduce the library maintenance surface, The Synchronous client is primarily auto-generated from the async codebase,
with a hand-written layer over top

The auto-generated surface lives at `_sync/_autogen.py`, and can be generated with `sync_surface_generator.py`:

```
# from repo root:
nox -s generate_sync
```

The generator preforms ast transformations to replace all async API calls with
corresponding sync-friendly ones. It has also been configured to selectively drop
or replace certain methods that do not make sense in a sync context
(for example, background task coroutines)

The generated classes are all abstract, and can not be instantiated directly. Instead,
concrete subclasses are maintained in `_sync/_concrete.py`. This gives library maintainers
a chance to override certain auto-generated functionality with hand-written logic.

## Tests

The auto-generator also generates system tests at `tests/system/test_sync_autogen.py`.

Due to more granular functionality differences between sync and async,
unit tests are not generated, and must be maintained by hand.

There is an additional unit test in `tests/unit/sync/test_sync_up_to_date.py`, that will
raise an error if there are changes in the async surface that have not been saved
to the sync autogen layer. This will ensure that the two surfaces stay consistent over time.
