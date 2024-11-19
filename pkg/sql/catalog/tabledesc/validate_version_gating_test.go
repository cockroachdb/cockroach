// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc_test

import (
	"context"
	gosql "database/sql"
	"encoding/hex"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// This file complements "validate_test.go" file but is dedicated to testing
// validation logic added with a version gate. In general, adding new validation
// should almost always come with an accompanying version gate, so that the
// validation is disabled when the cluster is in a mixed version state. This
// helps avoid the disastrous situation where an existing corruption is caught
// by the newly added validation, and it starts to scream and block user
// workload.
//
// This test file therefore should contain tests where, if we added some
// validation logic and gated it behind a cluster version, then we ensure a
// mixed version cluster with such a corruption
//   1. can accept SQL reads/writes
//   2. can report this corruption from `invalid_objects` vtable
//   3. cannot upgrade the cluster version (because the precondition check on `invalid_objects` fails)

// mustInsertDescToDB decode a table descriptor from a hex-encoded string and insert
// it into the database.
func mustInsertDescToDB(
	ctx context.Context, t *testing.T, db *gosql.DB, hexEncodedDescriptor string,
) {
	tdb := sqlutils.MakeSQLRunner(db)
	var parentID, parentSchemaID descpb.ID
	tdb.Exec(t, "CREATE TABLE temp_tbl()")
	tdb.QueryRow(t, `SELECT "parentID", "parentSchemaID" FROM system.namespace WHERE name = 'temp_tbl'`).
		Scan(&parentID, &parentSchemaID)
	tdb.Exec(t, `DROP TABLE temp_tbl;`)

	// Decode descriptor
	decodedDescriptor, err := hex.DecodeString(hexEncodedDescriptor)
	require.NoError(t, err)
	b, err := descbuilder.FromBytesAndMVCCTimestamp(decodedDescriptor, hlc.Timestamp{WallTime: 1})
	require.NoError(t, err)
	tableDesc := b.(tabledesc.TableDescriptorBuilder).BuildCreatedMutableTable()

	// Modify this descriptor's parentID and parentSchemaID as it could be
	// different from when the descriptor was serialized.
	tableDesc.ParentID = parentID
	tableDesc.UnexposedParentSchemaID = parentSchemaID

	// Insert the descriptor into test cluster.
	require.NoError(t, sqlutils.InjectDescriptors(
		ctx, db, []*descpb.Descriptor{tableDesc.DescriptorProto()}, true, /* force */
	))
}

// Appease the linter when no tests are currently necessary.
var _ = mustInsertDescToDB
