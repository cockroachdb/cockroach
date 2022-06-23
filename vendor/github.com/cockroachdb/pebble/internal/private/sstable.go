// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package private

import "github.com/cockroachdb/pebble/internal/base"

// SSTableCacheOpts is a hook for specifying cache options to
// sstable.NewReader.
var SSTableCacheOpts func(cacheID uint64, fileNum base.FileNum) interface{}

// SSTableRawTombstonesOpt is a sstable.Reader option for disabling
// fragmentation of the range tombstones returned by
// sstable.Reader.NewRangeDelIter(). Used by debug tools to get a raw view of
// the tombstones contained in an sstable.
var SSTableRawTombstonesOpt interface{}

// SSTableWriterDisableKeyOrderChecks is a hook for disabling the key ordering
// invariant check performed by sstable.Writer. It is intended for internal use
// only in the construction of invalid sstables for testing. See
// tool/make_test_sstables.go.
var SSTableWriterDisableKeyOrderChecks func(interface{})

// SSTableInternalTableOpt is an sstable.Writer option that sets properties for
// sstables being created by the db itself (i.e. through flushes and
// compactions), as opposed to those meant for ingestion.
var SSTableInternalTableOpt interface{}
