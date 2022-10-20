// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package splitpurpose

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// SplitPurpose maps SQL split causes to KV roachpb.AdminSplitRequest_SplitClass.
type SplitPurpose = roachpb.AdminSplitRequest_SplitClass

const (
	// SplitPurposeBackup is a split caused by a backup.
	SplitPurposeBackup = roachpb.AdminSplitRequest_INGESTION
	// SplitPurposeImport is a split caused by an import.
	SplitPurposeImport = roachpb.AdminSplitRequest_INGESTION
	// SplitPurposeSchema is a split caused by a schema change.
	SplitPurposeSchema = roachpb.AdminSplitRequest_INGESTION
	// SplitPurposeCreateTenant is a split caused by tenant creation.
	SplitPurposeCreateTenant = roachpb.AdminSplitRequest_INGESTION
	// SplitPurposeManualSplit is a split caused by a manual action.
	SplitPurposeManualSplit = roachpb.AdminSplitRequest_ARBITRARY
	// SplitPurposeManualSplitTest is a split caused by a manual action test.
	SplitPurposeManualSplitTest = roachpb.AdminSplitRequest_INGESTION
)
