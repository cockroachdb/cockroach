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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// WaitForNoRunningSchemaChanges schema changes waits for no schema changes
// to exist.
func WaitForNoRunningSchemaChanges(t *testing.T, tdb *sqlutils.SQLRunner) {
	tdb.CheckQueryResultsRetry(t, `
SELECT count(*) 
FROM [SHOW JOBS] 
WHERE job_type = 'SCHEMA CHANGE' 
  AND status NOT IN ('succeeded', 'failed', 'aborted')`,
		[][]string{{"0"}})
}

// ReadDescriptorsFromDB reads the set of d
func ReadDescriptorsFromDB(
	ctx context.Context, t *testing.T, tdb *sqlutils.SQLRunner,
) nstree.MutableCatalog {
	var cb nstree.MutableCatalog
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
		b := descbuilder.NewBuilderWithMVCCTimestamp(descProto, ts)
		b.RunPostDeserializationChanges()
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

		cb.UpsertDescriptorEntry(desc)
	}
	return cb
}

// ReadNamespaceFromDB reads the namespace entries from tdb.
func ReadNamespaceFromDB(t *testing.T, tdb *sqlutils.SQLRunner) nstree.MutableCatalog {
	// Fetch namespace state.
	var cb nstree.MutableCatalog
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
		cb.UpsertNamespaceEntry(key, descpb.ID(id))
	}
	return cb
}

// ReadCurrentDatabaseFromDB reads the current database from tdb.
func ReadCurrentDatabaseFromDB(t *testing.T, tdb *sqlutils.SQLRunner) (db string) {
	tdb.QueryRow(t, `SELECT current_database()`).Scan(&db)
	return db
}

// ReadSessionDataFromDB reads the session data out of tdb and then
// allows the caller to modify it with the passed function.
func ReadSessionDataFromDB(
	t *testing.T, tdb *sqlutils.SQLRunner, override func(sd *sessiondata.SessionData),
) (sd sessiondata.SessionData) {
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
	sd = *sessionData
	override(&sd)
	return sd
}
