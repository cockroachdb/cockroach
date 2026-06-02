// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package schematestutils is a utility package for constructing schema objects
// in the context of cdc.
package schematestutils

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// MakeTableDesc makes a generic table descriptor with the provided properties.
func MakeTableDesc(
	tableID descpb.ID,
	version descpb.DescriptorVersion,
	modTime hlc.Timestamp,
	cols int,
	primaryKeyIndex int,
) catalog.TableDescriptor {
	td := descpb.TableDescriptor{
		Name:             "foo",
		ID:               tableID,
		Version:          version,
		ModificationTime: modTime,
		NextColumnID:     1,
		PrimaryIndex: descpb.IndexDescriptor{
			ID: descpb.IndexID(primaryKeyIndex),
		},
	}
	for i := 0; i < cols; i++ {
		td.Columns = append(td.Columns, *MakeColumnDesc(td.NextColumnID))
		td.NextColumnID++
	}
	return tabledesc.NewBuilder(&td).BuildImmutableTable()
}

// MakeColumnDesc makes a generic column descriptor with the provided id.
func MakeColumnDesc(id descpb.ColumnID) *descpb.ColumnDescriptor {
	return &descpb.ColumnDescriptor{
		Name:        "c" + strconv.Itoa(int(id)),
		ID:          id,
		Type:        types.Bool,
		DefaultExpr: new(descpb.Expression("true")),
	}
}

// SetLocalityRegionalByRow sets the LocalityConfig of the table
// descriptor such that desc.IsLocalityRegionalByRow will return true.
func SetLocalityRegionalByRow(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().LocalityConfig = &catpb.LocalityConfig{
		Locality: &catpb.LocalityConfig_RegionalByRow_{
			RegionalByRow: &catpb.LocalityConfig_RegionalByRow{},
		},
	}
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddColumnDropBackfillMutation adds a mutation to desc to drop a column.
// Yes, this does modify an immutable.
func AddColumnDropBackfillMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_DROP,
		Descriptor_: &descpb.DescriptorMutation_Column{Column: MakeColumnDesc(desc.GetNextColumnID() - 1)},
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddNewColumnBackfillMutation adds a mutation to desc to add a column.
// Yes, this does modify an immutable.
func AddNewColumnBackfillMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Column{Column: MakeColumnDesc(desc.GetNextColumnID())},
		State:       descpb.DescriptorMutation_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_ADD,
		MutationID:  0,
		Rollback:    false,
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddPrimaryKeySwapMutation adds a mutation to desc to do a primary key swap.
// Yes, this does modify an immutable.
func AddPrimaryKeySwapMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_ADD,
		Descriptor_: &descpb.DescriptorMutation_PrimaryKeySwap{PrimaryKeySwap: &descpb.PrimaryKeySwap{}},
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddNewIndexMutation adds a mutation to desc to add an index.
// Yes, this does modify an immutable.
func AddNewIndexMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_ADD,
		Descriptor_: &descpb.DescriptorMutation_Index{Index: &descpb.IndexDescriptor{}},
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// AddDropIndexMutation adds a mutation to desc to drop an index.
// Yes, this does modify an immutable.
func AddDropIndexMutation(desc catalog.TableDescriptor) catalog.TableDescriptor {
	desc.TableDesc().Mutations = append(desc.TableDesc().Mutations, descpb.DescriptorMutation{
		State:       descpb.DescriptorMutation_WRITE_ONLY,
		Direction:   descpb.DescriptorMutation_DROP,
		Descriptor_: &descpb.DescriptorMutation_Index{Index: &descpb.IndexDescriptor{}},
	})
	return tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable()
}

// FetchDescVersionModificationTime fetches the `ModificationTime` of the
// specified `version` of `tableName`'s table descriptor.
//
// Prefer FetchModificationTimeOfFirstMatchingDesc when the caller can describe
// the desired schema state with a predicate: hardcoded version numbers are
// brittle because the schema changer's intermediate steps can shift between
// releases or even between runs (e.g. under stress).
func FetchDescVersionModificationTime(
	t testing.TB,
	s serverutils.ApplicationLayerInterface,
	dbName string,
	schemaName string,
	tableName string,
	version int,
) hlc.Timestamp {
	ts := fetchFirstMatchingDescModTime(t, s, dbName, schemaName, tableName,
		func(tbl catalog.TableDescriptor) bool {
			return int(tbl.GetVersion()) == version
		})
	if ts.IsEmpty() {
		t.Fatalf("couldn't find table desc for version %d", version)
	}
	return ts
}

// FetchModificationTimeOfFirstMatchingDesc returns the ModificationTime of the
// numerically smallest table-descriptor version satisfying pred.
//
// This lets a test pin down the timestamp at which a table reached a particular
// schema state (e.g. "column b has been dropped and no mutations are pending")
// without hardcoding a descriptor version number. Hardcoded versions are
// brittle: under race or with future schema-changer changes, a schema change
// may produce a different number of intermediate descriptor versions, causing
// the lookup to either fail or return the wrong version's timestamp.
//
// The predicate should describe a transient state that is reached exactly once
// during the test (e.g. by referring to the public column set), so that the
// "first matching version" corresponds to the moment the schema change of
// interest committed.
//
// Fails the test if no version matches.
func FetchModificationTimeOfFirstMatchingDesc(
	t testing.TB,
	s serverutils.ApplicationLayerInterface,
	dbName string,
	schemaName string,
	tableName string,
	pred func(catalog.TableDescriptor) bool,
) hlc.Timestamp {
	ts := fetchFirstMatchingDescModTime(t, s, dbName, schemaName, tableName, pred)
	if ts.IsEmpty() {
		// Re-scan to produce a useful diagnostic listing every version observed.
		var observed []string
		forEachTableDescVersion(t, s, dbName, schemaName, tableName,
			func(tbl catalog.TableDescriptor) {
				cols := make([]string, 0, len(tbl.PublicColumns()))
				for _, c := range tbl.PublicColumns() {
					cols = append(cols, c.GetName())
				}
				observed = append(observed,
					fmt.Sprintf("v%d cols=%v mutations=%d",
						tbl.GetVersion(), cols, len(tbl.AllMutations())))
			})
		t.Fatalf("no descriptor version matched predicate; observed versions:\n  %s",
			strings.Join(observed, "\n  "))
	}
	return ts
}

// fetchFirstMatchingDescModTime returns the ModificationTime of the lowest
// descriptor version satisfying pred, or an empty timestamp if none match.
func fetchFirstMatchingDescModTime(
	t testing.TB,
	s serverutils.ApplicationLayerInterface,
	dbName, schemaName, tableName string,
	pred func(catalog.TableDescriptor) bool,
) hlc.Timestamp {
	var bestVersion descpb.DescriptorVersion
	var bestTS hlc.Timestamp
	forEachTableDescVersion(t, s, dbName, schemaName, tableName,
		func(tbl catalog.TableDescriptor) {
			if !pred(tbl) {
				return
			}
			if bestTS.IsEmpty() || tbl.GetVersion() < bestVersion {
				bestVersion = tbl.GetVersion()
				bestTS = tbl.GetModificationTime()
			}
		})
	return bestTS
}

// forEachTableDescVersion invokes visit for every historical version of the
// named table descriptor found in the descriptor table.
func forEachTableDescVersion(
	t testing.TB,
	s serverutils.ApplicationLayerInterface,
	dbName, schemaName, tableName string,
	visit func(catalog.TableDescriptor),
) {
	db := s.SQLConn(t, serverutils.DBName(dbName))
	tblKey := s.Codec().IndexPrefix(keys.DescriptorTableID, keys.DescriptorTablePrimaryKeyIndexID)
	tableID := sqlutils.QueryTableID(t, db, dbName, schemaName, tableName)
	req := &kvpb.ExportRequest{
		RequestHeader: kvpb.RequestHeader{Key: tblKey, EndKey: tblKey.PrefixEnd()},
		MVCCFilter:    kvpb.MVCCFilter_All,
		StartTime:     hlc.Timestamp{},
	}
	hh := kvpb.Header{Timestamp: hlc.NewClockForTesting(nil).Now()}
	res, pErr := kv.SendWrappedWith(context.Background(),
		s.DB().NonTransactionalSender(), hh, req)
	if pErr != nil {
		t.Fatal(pErr.GoError())
	}
	for _, file := range res.(*kvpb.ExportResponse).Files {
		func() {
			it, err := storage.NewMemSSTIterator(file.SST, false /* verify */, storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsAndRanges,
				LowerBound: keys.MinKey,
				UpperBound: keys.MaxKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			for it.SeekGE(storage.NilKey); ; it.Next() {
				if ok, err := it.Valid(); err != nil {
					t.Fatal(err)
				} else if !ok {
					return
				}
				k := it.UnsafeKey()
				if _, hasRange := it.HasPointAndRange(); hasRange {
					t.Fatalf("unexpected MVCC range key at %s", k)
				}
				remaining, _, _, err := s.Codec().DecodeIndexPrefix(k.Key)
				if err != nil {
					t.Fatal(err)
				}
				_, descID, err := encoding.DecodeUvarintAscending(remaining)
				if err != nil {
					t.Fatal(err)
				}
				if descID != uint64(tableID) {
					continue
				}
				unsafeValue, err := it.UnsafeValue()
				require.NoError(t, err)
				if unsafeValue == nil {
					t.Fatal(errors.New(`value was dropped or truncated`))
				}
				value := roachpb.Value{RawBytes: unsafeValue, Timestamp: k.Timestamp}
				b, err := descbuilder.FromSerializedValue(&value)
				if err != nil {
					t.Fatal(err)
				}
				require.NotNil(t, b)
				if b.DescriptorType() == catalog.Table {
					visit(b.BuildImmutable().(catalog.TableDescriptor))
				}
			}
		}()
	}
}
