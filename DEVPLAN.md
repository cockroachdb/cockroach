# Dev Plan

TODO(jeffswenson): delete this file when commiting

## Testing

1. Add a test to pkg/kv/kv_client_lww_test.go that verifies the LWW semantics
   end to end.
2. Extend the mvcc_history_tests.

## Refactoring

1. Should the logic be moved into mvccPut or should there be a single helper
   used for handling LWW? 
