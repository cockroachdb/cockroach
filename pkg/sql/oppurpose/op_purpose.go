// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package oppurpose

import "github.com/cockroachdb/cockroach/pkg/roachpb"

const (
	// SplitBackup is a split caused by a backup.
	SplitBackup = roachpb.AdminSplitRequest_INGESTION
	// SplitImport is a split caused by an import.
	SplitImport = roachpb.AdminSplitRequest_INGESTION
	// SplitSchema is a split caused by a schema change.
	SplitSchema = roachpb.AdminSplitRequest_INGESTION
	// SplitCreateTenant is a split caused by tenant creation.
	SplitCreateTenant = roachpb.AdminSplitRequest_INGESTION
	// SplitManual is a split caused by a manual action.
	SplitManual = roachpb.AdminSplitRequest_ARBITRARY
	// SplitManualTest is a split caused by a manual action test.
	SplitManualTest = roachpb.AdminSplitRequest_INGESTION

	// UnsplitManual is a split caused by a manual action.
	UnsplitManual = roachpb.AdminUnsplitRequest_ARBITRARY
	// UnsplitGC is a split caused by the GC job.
	UnsplitGC = roachpb.AdminUnsplitRequest_ORGANIZATION

	// ScatterBackup is a split caused by a backup.
	ScatterBackup = roachpb.AdminScatterRequest_INGESTION
	// ScatterBulk is a split caused by a bulk operation.
	ScatterBulk = roachpb.AdminScatterRequest_INGESTION
	// ScatterSchema is a split caused by a schema change.
	ScatterSchema = roachpb.AdminScatterRequest_INGESTION
	// ScatterTruncate is a split caused by a truncation.
	ScatterTruncate = roachpb.AdminScatterRequest_INGESTION
	// ScatterManual is a split caused by a manual action.
	ScatterManual = roachpb.AdminScatterRequest_ARBITRARY
	// ScatterManualTest is a split caused by a manual action test.
	ScatterManualTest = roachpb.AdminScatterRequest_INGESTION
)
