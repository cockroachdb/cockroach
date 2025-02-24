// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package schematestutils is a utility package for constructing schema objects
// in the context of cdc.
package schematestutils

import (
	"context"
	"strconv"
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
	"github.com/gogo/protobuf/proto"
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
		DefaultExpr: proto.String("true"),
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
func FetchDescVersionModificationTime(
	t testing.TB,
	s serverutils.ApplicationLayerInterface,
	dbName string,
	schemaName string,
	tableName string,
	version int,
) hlc.Timestamp {
	db := serverutils.OpenDBConn(
		t, s.SQLAddr(), dbName, false, s.AppStopper())

	tblKey := s.Codec().TablePrefix(keys.DescriptorTableID)
	header := kvpb.RequestHeader{
		Key:    tblKey,
		EndKey: tblKey.PrefixEnd(),
	}
	dropColTblID := sqlutils.QueryTableID(t, db, dbName, schemaName, tableName)
	req := &kvpb.ExportRequest{
		RequestHeader: header,
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
		it, err := storage.NewMemSSTIterator(file.SST, false /* verify */, storage.IterOptions{
			KeyTypes:   storage.IterKeyTypePointsAndRanges,
			LowerBound: keys.MinKey,
			UpperBound: keys.MaxKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		//nolint:deferloop TODO(#137605)
		defer it.Close()
		for it.SeekGE(storage.NilKey); ; it.Next() {
			if ok, err := it.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				continue
			}
			k := it.UnsafeKey()
			if _, hasRange := it.HasPointAndRange(); hasRange {
				t.Fatalf("unexpected MVCC range key at %s", k)
			}
			remaining, _, _, err := s.Codec().DecodeIndexPrefix(k.Key)
			if err != nil {
				t.Fatal(err)
			}
			_, tableID, err := encoding.DecodeUvarintAscending(remaining)
			if err != nil {
				t.Fatal(err)
			}
			if tableID != uint64(dropColTblID) {
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
				tbl := b.BuildImmutable().(catalog.TableDescriptor)
				if int(tbl.GetVersion()) == version {
					return tbl.GetModificationTime()
				}
			}
		}
	}
	t.Fatal(errors.New(`couldn't find table desc for given version`))
	return hlc.Timestamp{}
}
