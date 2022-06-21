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

type StatementTimeName string
type WatchedTables map[descpb.ID]StatementTimeName
type Targets []Target
