// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"github.com/cockroachdb/pebble/sstable"
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

var readCategoryMap = [...]sstable.Category{
	UnknownReadCategory: sstable.RegisterCategory("crdb-unknown", sstable.LatencySensitiveQoSLevel),
	// TODO(sumeer): consider splitting batch-eval into two categories, for
	// latency sensitive and non latency sensitive.
	BatchEvalReadCategory:               sstable.RegisterCategory("batch-eval", sstable.LatencySensitiveQoSLevel),
	ScanRegularBatchEvalReadCategory:    sstable.RegisterCategory("scan-regular", sstable.LatencySensitiveQoSLevel),
	ScanBackgroundBatchEvalReadCategory: sstable.RegisterCategory("scan-background", sstable.NonLatencySensitiveQoSLevel),
	MVCCGCReadCategory:                  sstable.RegisterCategory("mvcc-gc", sstable.NonLatencySensitiveQoSLevel),
	RangeSnapshotReadCategory:           sstable.RegisterCategory("range-snap", sstable.NonLatencySensitiveQoSLevel),
	RangefeedReadCategory:               sstable.RegisterCategory("rangefeed", sstable.LatencySensitiveQoSLevel),
	ReplicationReadCategory:             sstable.RegisterCategory("replication", sstable.LatencySensitiveQoSLevel),
	IntentResolutionReadCategory:        sstable.RegisterCategory("intent-resolution", sstable.LatencySensitiveQoSLevel),
	BackupReadCategory:                  sstable.RegisterCategory("backup", sstable.NonLatencySensitiveQoSLevel),
}

// PebbleCategory returns the sstable.Category associated with the given ReadCategory.
func (c ReadCategory) PebbleCategory() sstable.Category {
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
