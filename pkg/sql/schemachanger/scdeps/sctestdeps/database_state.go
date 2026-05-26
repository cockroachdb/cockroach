// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctestdeps

import (
	"context"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// ReadDescriptorsFromDB reads the set of descriptors from tdb.
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
		b, err := descbuilder.FromBytesAndMVCCTimestamp(descBytes, ts)
		if err != nil {
			t.Fatal(err)
		}
		if err = b.RunPostDeserializationChanges(); err != nil {
			t.Fatal(err)
		}
		desc := b.BuildCreatedMutable()

		// Redact time-dependent fields.
		switch t := desc.(type) {
		case *dbdesc.Mutable:
			t.ModificationTime = hlc.Timestamp{}
		case *schemadesc.Mutable:
			t.ModificationTime = hlc.Timestamp{}
		case *tabledesc.Mutable:
			t.TableDescriptor.ModificationTime = hlc.Timestamp{}
			if t.TableDescriptor.CreateAsOfTime != (hlc.Timestamp{}) {
				t.TableDescriptor.CreateAsOfTime = hlc.Timestamp{WallTime: defaultOverriddenCreatedAt.UnixNano()}
			}
			if t.TableDescriptor.DropTime != 0 {
				t.TableDescriptor.DropTime = 1
			}
		case *typedesc.Mutable:
			t.TypeDescriptor.ModificationTime = hlc.Timestamp{}
		}

		cb.UpsertDescriptor(desc)
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
		cb.UpsertNamespaceEntry(key, descpb.ID(id), hlc.Timestamp{})
	}
	return cb
}

// ReadZoneConfigsFromDB reads the zone configs from tdb.
func ReadZoneConfigsFromDB(
	t *testing.T, tdb *sqlutils.SQLRunner, descCatalog nstree.Catalog,
) map[catid.DescID]catalog.ZoneConfig {
	zoneCfgMap := make(map[catid.DescID]catalog.ZoneConfig)
	require.NoError(t, descCatalog.ForEachDescriptor(func(desc catalog.Descriptor) error {
		zoneCfgRow := tdb.Query(t, `
SELECT config FROM system.zones WHERE id=$1
`,
			desc.GetID())
		defer zoneCfgRow.Close()
		once := false
		for zoneCfgRow.Next() {
			require.Falsef(t, once, "multiple zone config entries for descriptor")
			var bytes []byte
			require.NoError(t, zoneCfgRow.Scan(&bytes))
			var z zonepb.ZoneConfig
			require.NoError(t, protoutil.Unmarshal(bytes, &z))
			zoneCfgMap[desc.GetID()] = zone.NewZoneConfigWithRawBytes(&z, bytes)
			once = true
		}
		return nil
	}))
	return zoneCfgMap
}

// ReadCurrentDatabaseFromDB reads the current database from tdb.
func ReadCurrentDatabaseFromDB(t *testing.T, tdb *sqlutils.SQLRunner) (db string) {
	tdb.QueryRow(t, `SELECT current_database()`).Scan(&db)
	return db
}

// ReadSessionDataFromDB reads the session data out of tdb and then
// allows the caller to modify it with the passed function.
func ReadSessionDataFromDB(
	t *testing.T,
	tdb *sqlutils.SQLRunner,
	override func(sd *sessiondata.SessionData, localData sessiondatapb.LocalOnlySessionData),
) sessiondata.SessionData {
	hexSessionData := tdb.QueryStr(t, `SELECT encode(crdb_internal.serialize_session(), 'hex')`)
	if len(hexSessionData) == 0 {
		t.Fatal("Empty session data query results.")
	}
	sessionDataBytes, err := hex.DecodeString(hexSessionData[0][0])
	if err != nil {
		t.Fatal(err)
	}
	var m sessiondatapb.MigratableSession
	err = protoutil.Unmarshal(sessionDataBytes, &m)
	if err != nil {
		t.Fatal(err)
	}
	sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
	if err != nil {
		t.Fatal(err)
	}
	sd.SessionData = m.SessionData
	override(sd, m.LocalOnlySessionData)
	return *sd
}

// ReadCommentsFromDB reads all comments from system.comments table and return
// them as a CommentCache.
func ReadCommentsFromDB(t *testing.T, tdb *sqlutils.SQLRunner) map[catalogkeys.CommentKey]string {
	comments := make(map[catalogkeys.CommentKey]string)
	commentRows := tdb.QueryStr(t, `SELECT type, object_id, sub_id, comment FROM system.comments`)
	for _, row := range commentRows {
		typeVal, err := strconv.Atoi(row[0])
		if err != nil {
			t.Fatal(err)
		}
		commentType := catalogkeys.CommentType(typeVal)
		objID, err := strconv.Atoi(row[1])
		if err != nil {
			t.Fatal(err)
		}
		subID, err := strconv.Atoi(row[2])
		if err != nil {
			t.Fatal(err)
		}
		comment := row[3]
		commentKey := catalogkeys.MakeCommentKey(uint32(objID), uint32(subID), commentType)
		comments[commentKey] = comment
	}
	return comments
}
