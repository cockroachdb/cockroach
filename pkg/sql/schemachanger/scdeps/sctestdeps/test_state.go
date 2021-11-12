// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctestdeps

import (
	"context"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// testPartitionInfo tracks partitioning information
// for testing
type testPartitionInfo struct {
	tree.PartitionBy
}

// TestState is a backing struct used to implement all schema changer
// dependencies, like scbuild.Dependencies or scexec.Dependencies, for the
// purpose of facilitating end-to-end testing of the declarative schema changer.
type TestState struct {
	descriptors, syntheticDescriptors nstree.Map
	partitioningInfo                  map[descpb.ID]*testPartitionInfo
	namespace                         map[descpb.NameInfo]descpb.ID
	currentDatabase                   string
	phase                             scop.Phase
	sessionData                       sessiondata.SessionData
	statements                        []string
	testingKnobs                      *scexec.NewSchemaChangerTestingKnobs
	jobs                              []jobs.Record
}

// NewTestDependencies returns a TestState populated with the catalog state of
// the given test database handle.
func NewTestDependencies(
	ctx context.Context,
	t *testing.T,
	tdb *sqlutils.SQLRunner,
	testingKnobs *scexec.NewSchemaChangerTestingKnobs,
	statements []string,
) *TestState {

	s := &TestState{
		currentDatabase: "defaultdb",
		namespace:       make(map[descpb.NameInfo]descpb.ID),
		phase:           scop.StatementPhase,
		statements:      statements,
		testingKnobs:    testingKnobs,
	}
	// Wait for schema changes to complete.
	tdb.CheckQueryResultsRetry(t, `
SELECT count(*) 
FROM [SHOW JOBS] 
WHERE job_type = 'SCHEMA CHANGE' 
  AND status NOT IN ('succeeded', 'failed', 'aborted')`,
		[][]string{{"0"}})

	// Fetch descriptor state.
	{
		hexDescRows := tdb.QueryStr(t, `
SELECT encode(descriptor, 'hex'), crdb_internal_mvcc_timestamp 
FROM system.descriptor 
ORDER BY id`)
		for _, hexDescRow := range hexDescRows {
			descBytes, err := hex.DecodeString(hexDescRow[0])
			if err != nil {
				t.Fatal(err)
			}
			ts, err := hlc.ParseTimestamp(hexDescRow[1])
			if err != nil {
				t.Fatal(err)
			}
			descProto := &descpb.Descriptor{}
			err = protoutil.Unmarshal(descBytes, descProto)
			if err != nil {
				t.Fatal(err)
			}
			b := catalogkv.NewBuilderWithMVCCTimestamp(descProto, ts)
			err = b.RunPostDeserializationChanges(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			desc := b.BuildCreatedMutable()
			if desc.GetID() == keys.SystemDatabaseID || desc.GetParentID() == keys.SystemDatabaseID {
				continue
			}
			s.descriptors.Upsert(desc)
		}
	}

	// Fetch namespace state.
	{
		nsRows := tdb.QueryStr(t, `
SELECT "parentID", "parentSchemaID", name, id 
FROM system.namespace
ORDER BY id`)
		for _, nsRow := range nsRows {
			parentID, err := strconv.Atoi(nsRow[0])
			if err != nil {
				t.Fatal(err)
			}
			parentSchemaID, err := strconv.Atoi(nsRow[1])
			if err != nil {
				t.Fatal(err)
			}
			name := nsRow[2]
			id, err := strconv.Atoi(nsRow[3])
			if err != nil {
				t.Fatal(err)
			}
			if id == keys.SystemDatabaseID || parentID == keys.SystemDatabaseID {
				// Exclude system database and its objects from namespace state.
				continue
			}
			key := descpb.NameInfo{
				ParentID:       descpb.ID(parentID),
				ParentSchemaID: descpb.ID(parentSchemaID),
				Name:           name,
			}
			s.namespace[key] = descpb.ID(id)
		}
	}

	// Fetch current database state.
	{
		currdb := tdb.QueryStr(t, `SELECT current_database()`)
		if len(currdb) == 0 {
			t.Fatal("Empty current database query results.")
		}
		s.currentDatabase = currdb[0][0]
	}

	// Fetch session data.
	{
		hexSessionData := tdb.QueryStr(t, `SELECT encode(crdb_internal.serialize_session(), 'hex')`)
		if len(hexSessionData) == 0 {
			t.Fatal("Empty session data query results.")
		}
		sessionDataBytes, err := hex.DecodeString(hexSessionData[0][0])
		if err != nil {
			t.Fatal(err)
		}
		sessionDataProto := sessiondatapb.SessionData{}
		err = protoutil.Unmarshal(sessionDataBytes, &sessionDataProto)
		if err != nil {
			t.Fatal(err)
		}
		sessionData, err := sessiondata.UnmarshalNonLocal(sessionDataProto)
		if err != nil {
			t.Fatal(err)
		}
		s.sessionData = *sessionData
	}

	return s
}

// WithTxn simulates the execution of a transaction.
func (s *TestState) WithTxn(fn func(s *TestState)) {
	defer s.syntheticDescriptors.Clear()
	fn(s)
}
