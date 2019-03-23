// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestValidateCrossTableReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tests := []struct {
		err        string
		desc       catpb.TableDescriptor
		referenced []catpb.TableDescriptor
	}{
		// Foreign keys
		{
			err: `invalid foreign key: missing table=52 index=2: descriptor not found`,
			desc: catpb.TableDescriptor{
				ID: 51,
				PrimaryIndex: catpb.IndexDescriptor{
					ID:         1,
					ForeignKey: catpb.ForeignKeyReference{Table: 52, Index: 2},
				},
			},
			referenced: nil,
		},
		{
			err: `invalid foreign key: missing table=baz index=2: index-id "2" does not exist`,
			desc: catpb.TableDescriptor{
				ID: 51,
				PrimaryIndex: catpb.IndexDescriptor{
					ID:         1,
					ForeignKey: catpb.ForeignKeyReference{Table: 52, Index: 2},
				},
			},
			referenced: []catpb.TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `missing fk back reference to "foo"@"bar" from "baz"@"qux"`,
			desc: catpb.TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: catpb.IndexDescriptor{
					ID:         1,
					Name:       "bar",
					ForeignKey: catpb.ForeignKeyReference{Table: 52, Index: 2},
				},
			},
			referenced: []catpb.TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: catpb.IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{
			err: `invalid fk backreference table=52 index=2: descriptor not found`,
			desc: catpb.TableDescriptor{
				ID: 51,
				PrimaryIndex: catpb.IndexDescriptor{
					ID:           1,
					ReferencedBy: []catpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
		},
		{
			err: `invalid fk backreference table=baz index=2: index-id "2" does not exist`,
			desc: catpb.TableDescriptor{
				ID: 51,
				PrimaryIndex: catpb.IndexDescriptor{
					ID:           1,
					ReferencedBy: []catpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			referenced: []catpb.TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `broken fk backward reference from "foo"@"bar" to "baz"@"qux"`,
			desc: catpb.TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: catpb.IndexDescriptor{
					ID:           1,
					Name:         "bar",
					ReferencedBy: []catpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			referenced: []catpb.TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: catpb.IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},

		// Interleaves
		{
			err: `invalid interleave: missing table=52 index=2: descriptor not found`,
			desc: catpb.TableDescriptor{
				ID: 51,
				PrimaryIndex: catpb.IndexDescriptor{
					ID: 1,
					Interleave: catpb.InterleaveDescriptor{Ancestors: []catpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			referenced: nil,
		},
		{
			err: `invalid interleave: missing table=baz index=2: index-id "2" does not exist`,
			desc: catpb.TableDescriptor{
				ID: 51,
				PrimaryIndex: catpb.IndexDescriptor{
					ID: 1,
					Interleave: catpb.InterleaveDescriptor{Ancestors: []catpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			referenced: []catpb.TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `missing interleave back reference to "foo"@"bar" from "baz"@"qux"`,
			desc: catpb.TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: catpb.IndexDescriptor{
					ID:   1,
					Name: "bar",
					Interleave: catpb.InterleaveDescriptor{Ancestors: []catpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			referenced: []catpb.TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: catpb.IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{
			err: `invalid interleave backreference table=52 index=2: descriptor not found`,
			desc: catpb.TableDescriptor{
				ID: 51,
				PrimaryIndex: catpb.IndexDescriptor{
					ID:            1,
					InterleavedBy: []catpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
		},
		{
			err: `invalid interleave backreference table=baz index=2: index-id "2" does not exist`,
			desc: catpb.TableDescriptor{
				ID: 51,
				PrimaryIndex: catpb.IndexDescriptor{
					ID:            1,
					InterleavedBy: []catpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			referenced: []catpb.TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `broken interleave backward reference from "foo"@"bar" to "baz"@"qux"`,
			desc: catpb.TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: catpb.IndexDescriptor{
					ID:            1,
					Name:          "bar",
					InterleavedBy: []catpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			referenced: []catpb.TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: catpb.IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
	}

	{
		var v roachpb.Value
		desc := &catpb.Descriptor{Union: &catpb.Descriptor_Database{}}
		if err := v.SetProto(desc); err != nil {
			t.Fatal(err)
		}
		if err := kvDB.Put(ctx, sqlbase.MakeDescMetadataKey(0), &v); err != nil {
			t.Fatal(err)
		}
	}

	for i, test := range tests {
		for _, referencedDesc := range test.referenced {
			var v roachpb.Value
			desc := &catpb.Descriptor{Union: &catpb.Descriptor_Table{Table: &referencedDesc}}
			if err := v.SetProto(desc); err != nil {
				t.Fatal(err)
			}
			if err := kvDB.Put(ctx, sqlbase.MakeDescMetadataKey(referencedDesc.ID), &v); err != nil {
				t.Fatal(err)
			}
		}
		txn := client.NewTxn(ctx, kvDB, s.NodeID(), client.RootTxn)
		if err := validateCrossReferences(ctx, &test.desc, txn); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, test.err, test.desc)
		} else if test.err != err.Error() && "internal error: "+test.err != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, test.err, err.Error())
		}
		for _, referencedDesc := range test.referenced {
			if err := kvDB.Del(ctx, sqlbase.MakeDescMetadataKey(referencedDesc.ID)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestValidatePartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		err  string
		desc catpb.TableDescriptor
	}{
		{"at least one of LIST or RANGE partitioning must be used",
			catpb.TableDescriptor{
				PrimaryIndex: catpb.IndexDescriptor{
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			catpb.TableDescriptor{
				PrimaryIndex: catpb.IndexDescriptor{
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			catpb.TableDescriptor{
				PrimaryIndex: catpb.IndexDescriptor{
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{Name: "p1", Values: [][]byte{{}}}},
					},
				},
			},
		},
		{"only one LIST or RANGE partitioning may used",
			catpb.TableDescriptor{
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{}},
						Range:      []catpb.PartitioningDescriptor_Range{{}},
					},
				},
			},
		},
		{"PARTITION name must be non-empty",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{}},
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: empty array",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{{
							Name: "p1", Values: [][]byte{{}},
						}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: int64 varint decoding failed: 0",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03}}},
						},
					},
				},
			},
		},
		{"PARTITION p1: superfluous data in encoded value",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02, 0x00}}},
						},
					},
				},
			},
		},
		{"partitions p1 and p2 overlap",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1, 1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC, catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						Range: []catpb.PartitioningDescriptor_Range{
							{Name: "p1", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
							{Name: "p2", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
						},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{Name: "p1", Values: [][]byte{{0x03, 0x04}}},
						},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{{
							Name:   "p1",
							Values: [][]byte{{0x03, 0x02}},
							Subpartitioning: catpb.PartitioningDescriptor{
								NumColumns: 1,
								List:       []catpb.PartitioningDescriptor_List{{Name: "p1_1", Values: [][]byte{{}}}},
							},
						}},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			catpb.TableDescriptor{
				Columns: []catpb.ColumnDescriptor{{ID: 1, Type: catpb.ColumnType{SemanticType: catpb.ColumnType_INT}}},
				PrimaryIndex: catpb.IndexDescriptor{
					ColumnIDs:        []catpb.ColumnID{1, 1},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC, catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{
								Name:   "p2",
								Values: [][]byte{{0x03, 0x04}},
								Subpartitioning: catpb.PartitioningDescriptor{
									NumColumns: 1,
									List: []catpb.PartitioningDescriptor_List{
										{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for i, test := range tests {
		err := test.desc.validatePartitioning()
		if !testutils.IsError(err, test.err) {
			t.Errorf(`%d: got "%v" expected "%v"`, i, err, test.err)
		}
	}
}
