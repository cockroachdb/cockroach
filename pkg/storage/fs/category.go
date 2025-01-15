// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
)

// ReadCategory is used to export metrics and maps to a QoS understood by
// Pebble.
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

var readCategoryMap = [...]block.Category{
	UnknownReadCategory: block.RegisterCategory("crdb-unknown", block.LatencySensitiveQoSLevel),
	// TODO(sumeer): consider splitting batch-eval into two categories, for
	// latency sensitive and non latency sensitive.
	BatchEvalReadCategory:               block.RegisterCategory("batch-eval", block.LatencySensitiveQoSLevel),
	ScanRegularBatchEvalReadCategory:    block.RegisterCategory("scan-regular", block.LatencySensitiveQoSLevel),
	ScanBackgroundBatchEvalReadCategory: block.RegisterCategory("scan-background", block.NonLatencySensitiveQoSLevel),
	MVCCGCReadCategory:                  block.RegisterCategory("mvcc-gc", block.NonLatencySensitiveQoSLevel),
	RangeSnapshotReadCategory:           block.RegisterCategory("range-snap", block.NonLatencySensitiveQoSLevel),
	RangefeedReadCategory:               block.RegisterCategory("rangefeed", block.LatencySensitiveQoSLevel),
	ReplicationReadCategory:             block.RegisterCategory("replication", block.LatencySensitiveQoSLevel),
	IntentResolutionReadCategory:        block.RegisterCategory("intent-resolution", block.LatencySensitiveQoSLevel),
	BackupReadCategory:                  block.RegisterCategory("backup", block.NonLatencySensitiveQoSLevel),
}

// PebbleCategory returns the block.Category associated with the given ReadCategory.
func (c ReadCategory) PebbleCategory() block.Category {
	return readCategoryMap[c]
}

const (
	// UnspecifiedWriteCategory are disk writes that do not belong to any existing category.
	UnspecifiedWriteCategory = vfs.WriteCategoryUnspecified
	// RaftSnapshotWriteCategory are disk writes related to Raft snapshots.
	RaftSnapshotWriteCategory = "raft-snapshot"
	// SQLColumnSpillWriteCategory are disk writes related to disk spills caused SQL column spill.
	SQLColumnSpillWriteCategory = "sql-col-spill"
	// PebbleIngestionWriteCategory are disk writes related to Pebble SSTable ingestion.
	PebbleIngestionWriteCategory = "pebble-ingestion"
	// CRDBLogWriteCategory are disk writes related to Cockroach logs.
	CRDBLogWriteCategory = "crdb-log"
	// EncryptionRegistryWriteCategory are disk writes related to the EAR registry.
	EncryptionRegistryWriteCategory = "encryption-registry"
)
