// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// SystemTableIDResolver is a convenience interface used to resolve the table ID
// for a system table with a dynamic ID.
type SystemTableIDResolver interface {
	LookupSystemTableID(ctx context.Context, tableName string) (descpb.ID, error)
}
