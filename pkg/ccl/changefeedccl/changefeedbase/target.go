// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// Target provides a version-agnostic wrapper around jobspb.ChangefeedTargetSpecification.
type Target struct {
	Type              jobspb.ChangefeedTargetSpecification_TargetType
	TableID           descpb.ID
	FamilyName        string
	StatementTimeName StatementTimeName
}

// StatementTimeName is the original way a table was referred to when it was added to
// the changefeed, possibly modified by WITH options.
type StatementTimeName string

// WatchedTables tracks the StatementTimeName for each table id.
type WatchedTables map[descpb.ID]StatementTimeName

// Targets is the complete list of target specifications for a changefeed.
type Targets []Target
