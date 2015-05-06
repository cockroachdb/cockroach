#!/bin/bash

PKGS=$(echo github.com/cockroachdb/cockroach/{storage,kv})

eg -t eg/store.go -w $PKGS
eg -t eg/range.go -w $PKGS

gorename -from '"github.com/cockroachdb/cockroach/storage".Store.ExecuteCmd' -to ObsoleteExecuteCmd
gorename -from '"github.com/cockroachdb/cockroach/storage".Store.ExecuteCall' -to ExecuteCmd

gorename -from '"github.com/cockroachdb/cockroach/storage".Range.AddCmd' -to ObsoleteAddCmd
gorename -from '"github.com/cockroachdb/cockroach/storage".Range.AddCall' -to AddCmd
