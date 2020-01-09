// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package internal

// Silence unused warnings. The linter doesn't notice that these are test
// functions because their file is not named *_test.go. We don't want it
// to be because then it would be run by `go test`.
var _ = TestBTree
var _ = TestBTreeSeek
var _ = TestBTreeSeekOverlap
var _ = TestBTreeSeekOverlapRandom
var _ = TestBTreeCloneConcurrentOperations
var _ = TestBTreeCmp
var _ = TestIterStack
var _ = BenchmarkBTreeInsert
var _ = BenchmarkBTreeDelete
var _ = BenchmarkBTreeDeleteInsert
var _ = BenchmarkBTreeDeleteInsertCloneOnce
var _ = BenchmarkBTreeDeleteInsertCloneEachTime
var _ = BenchmarkBTreeMakeIter
var _ = BenchmarkBTreeIterSeekGE
var _ = BenchmarkBTreeIterSeekLT
var _ = BenchmarkBTreeIterFirstOverlap
var _ = BenchmarkBTreeIterNext
var _ = BenchmarkBTreeIterPrev
var _ = BenchmarkBTreeIterNextOverlap
var _ = BenchmarkBTreeIterOverlapScan
