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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
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
	testingKnobs                      *scrun.TestingKnobs
	jobs                              []jobs.Record
	jobCounter                        int
	txnCounter                        int
	sideEffectLogBuffer               strings.Builder
}

// NewTestDependencies returns a TestState populated with the catalog state of
// the given test database handle.
func NewTestDependencies(
	ctx context.Context,
	t *testing.T,
	tdb *sqlutils.SQLRunner,
	testingKnobs *scrun.TestingKnobs,
	statements []string,
) *TestState {

	s := &TestState{
		namespace:    make(map[descpb.NameInfo]descpb.ID),
		statements:   statements,
		testingKnobs: testingKnobs,
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

			// Redact time-dependent fields.
			switch t := desc.(type) {
			case *dbdesc.Mutable:
				t.ModificationTime = hlc.Timestamp{}
				t.DefaultPrivileges = nil
			case *schemadesc.Mutable:
				t.ModificationTime = hlc.Timestamp{}
			case *tabledesc.Mutable:
				t.TableDescriptor.ModificationTime = hlc.Timestamp{}
				if t.TableDescriptor.CreateAsOfTime != (hlc.Timestamp{}) {
					t.TableDescriptor.CreateAsOfTime = hlc.Timestamp{WallTime: 1}
				}
				if t.TableDescriptor.DropTime != 0 {
					t.TableDescriptor.DropTime = 1
				}
			case *typedesc.Mutable:
				t.TypeDescriptor.ModificationTime = hlc.Timestamp{}
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
		// For setting up a builder inside tests we will ensure that the new schema
		// changer will allow non-fully implemented operations.
		s.sessionData.NewSchemaChangerMode = sessiondatapb.UseNewSchemaChangerUnsafe
	}

	return s
}

// LogSideEffectf writes an entry to the side effect log, to keep track of any
// state changes which would have occurred in real dependencies.
func (s *TestState) LogSideEffectf(fmtstr string, args ...interface{}) {
	s.sideEffectLogBuffer.WriteString(fmt.Sprintf(fmtstr, args...))
	s.sideEffectLogBuffer.WriteRune('\n')
}

// SideEffectLog returns the contents of the side effect log.
// See LogSideEffectf for details.
func (s *TestState) SideEffectLog() string {
	return s.sideEffectLogBuffer.String()
}

// WithTxn simulates the execution of a transaction.
func (s *TestState) WithTxn(fn func(s *TestState)) {
	s.txnCounter++
	defer s.syntheticDescriptors.Clear()
	defer s.LogSideEffectf("commit transaction #%d", s.txnCounter)
	s.LogSideEffectf("begin transaction #%d", s.txnCounter)
	fn(s)
}

// IncrementPhase sets the state to the next phase.
func (s *TestState) IncrementPhase() {
	s.phase++
}

// JobRecord returns the job record in the fake job registry for the given job
// ID, if it exists, nil otherwise.
func (s *TestState) JobRecord(jobID jobspb.JobID) *jobs.Record {
	idx := int(jobID) - 1
	if idx < 0 || idx >= len(s.jobs) {
		return nil
	}
	return &s.jobs[idx]
}
