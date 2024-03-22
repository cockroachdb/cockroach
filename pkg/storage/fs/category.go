// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fs

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// ReadCategory is used to export metrics and maps to a QoS understood by
// Pebble. Categories are being introduced lazily, since more categories
// result in more metrics.
type ReadCategory int8

const (
	// UnknownReadCategory are requests that are not categorized. If the metric
	// for this category becomes a high fraction of reads, we will need to
	// investigate and break out more categories.
	UnknownReadCategory ReadCategory = iota
	// BatchEvalReadCategory includes evaluation of most BatchRequests. It
	// excludes scans and reverse scans. If scans and reverse scans are mixed
	// with other requests in a batch, we may currently assign the category
	// based on the first request.
	BatchEvalReadCategory
	// ScanRegularBatchEvalReadCategory are BatchRequest (reverse) scans that
	// have admission priority NormalPri or higher.
	ScanRegularBatchEvalReadCategory
	// ScanBackgroundBatchEvalReadCategory are BatchRequest (reverse) scans that
	// have admission priority lower than NormalPri. This includes backfill
	// scans for changefeeds (see changefeedccl/kvfeed/scanner.go, which sends
	// ScanRequests).
	ScanBackgroundBatchEvalReadCategory
	// MVCCGCReadCategory are reads for MVCC GC.
	MVCCGCReadCategory
	// RangeSnapshotReadCategory are reads for sending range snapshots.
	RangeSnapshotReadCategory
	// RangefeedReadCategory are reads for rangefeeds, including catchup scans.
	RangefeedReadCategory
	// ReplicationReadCategory are reads related to Raft replication.
	ReplicationReadCategory
	// IntentResolutionReadCategory are reads for intent resolution.
	IntentResolutionReadCategory
	// BackupReadCategory are reads for backups.
	BackupReadCategory
)

var readCategoryMap = map[ReadCategory]sstable.CategoryAndQoS{
	UnknownReadCategory: {Category: "crdb-unknown", QoSLevel: sstable.LatencySensitiveQoSLevel},
	// TODO(sumeer): consider splitting batch-eval into two categories, for
	// latency sensitive and non latency sensitive.
	BatchEvalReadCategory: {Category: "batch-eval", QoSLevel: sstable.LatencySensitiveQoSLevel},
	ScanRegularBatchEvalReadCategory: {
		Category: "scan-regular", QoSLevel: sstable.LatencySensitiveQoSLevel},
	ScanBackgroundBatchEvalReadCategory: {Category: "scan-background", QoSLevel: sstable.NonLatencySensitiveQoSLevel},
	MVCCGCReadCategory:                  {Category: "mvcc-gc", QoSLevel: sstable.NonLatencySensitiveQoSLevel},
	RangeSnapshotReadCategory: {
		Category: "range-snap", QoSLevel: sstable.NonLatencySensitiveQoSLevel},
	RangefeedReadCategory: {
		Category: "rangefeed", QoSLevel: sstable.LatencySensitiveQoSLevel},
	ReplicationReadCategory: {Category: "replication", QoSLevel: sstable.LatencySensitiveQoSLevel},
	IntentResolutionReadCategory: {
		Category: "intent-resolution", QoSLevel: sstable.LatencySensitiveQoSLevel},
	BackupReadCategory: {
		Category: "backup", QoSLevel: sstable.NonLatencySensitiveQoSLevel},
}

func GetCategoryAndQoS(c ReadCategory) sstable.CategoryAndQoS {
	categoryAndQoS, ok := readCategoryMap[c]
	if !ok {
		panic(errors.AssertionFailedf("unknown category %d", c))
	}
	return categoryAndQoS
}

const (
	UnspecifiedWriteCategory        = vfs.WriteCategoryUnspecified
	RaftSnapshotWriteCategory       = "raft-snapshot"
	SQLColumnSpillWriteCategory     = "sql-col-spill"
	PebbleIngestionWriteCategory    = "pebble-ingestion"
	CRDBLogWriteCategory            = "crdb-log"
	EncryptionRegistryWriteCategory = "encryption-registry"
)
