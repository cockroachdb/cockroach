// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecspan

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSpanAssembler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer testMemMonitor.Stop(ctx)
	nTuples := 3 * coldata.BatchSize()
	memAcc := testMemMonitor.MakeBoundAccount()
	testMemAcc := &memAcc
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, testMemAcc, testColumnFactory)
	defer testMemAcc.Close(ctx)
	rng, _ := randutil.NewTestRand()
	typs := []*types.T{types.Int, types.Bytes, types.Decimal}

	for _, useColFamilies := range []bool{true, false} {
		name := "WithColFamilies"
		if !useColFamilies {
			name = "NoColFamilies"
		}
		t.Run(name, func(t *testing.T) {
			for _, sizeLimit := range []int{
				1,       // 1 byte
				1 << 10, // 1 KB
				1 << 20, // 1 MB
			} {
				t.Run(fmt.Sprintf("sizeLimit=%d", sizeLimit), func(t *testing.T) {
					for _, useSel := range []bool{true, false} {
						t.Run(fmt.Sprintf("sel=%v", useSel), func(t *testing.T) {
							probOfOmittingRow := 0.0
							if useSel {
								probOfOmittingRow = 0.3
							}
							sel := coldatatestutils.RandomSel(rng, coldata.BatchSize(), probOfOmittingRow)
							testTable := makeTable(useColFamilies)
							neededColumns := util.MakeFastIntSet(1, 2, 3, 4)

							cols := make([]coldata.Vec, len(typs))
							for i, typ := range typs {
								cols[i] = testAllocator.NewMemColumn(typ, nTuples)
							}
							for i := range typs {
								coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
									Rand:            rng,
									Vec:             cols[i],
									N:               nTuples,
									NullProbability: 0, // Primary key columns are non-null.
								})
							}
							source := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, nTuples)
							source.Init(ctx)
							oracleSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, nTuples)
							oracleSource.Init(ctx)
							converter := colconv.NewAllVecToDatumConverter(len(typs))

							var builder span.Builder
							builder.Init(&evalCtx, keys.TODOSQLCodec, testTable, testTable.GetPrimaryIndex())
							splitter := span.MakeSplitter(testTable, testTable.GetPrimaryIndex(), neededColumns)

							var fetchSpec descpb.IndexFetchSpec
							if err := rowenc.InitIndexFetchSpec(
								&fetchSpec, keys.TODOSQLCodec, testTable, testTable.GetPrimaryIndex(), nil, /* fetchedColumnIDs */
							); err != nil {
								t.Fatal(err)
							}

							colBuilder := NewColSpanAssembler(
								keys.TODOSQLCodec,
								testAllocator,
								&fetchSpec,
								splitter.FamilyIDs(),
								typs,
							)
							defer func() {
								colBuilder.Close()
								colBuilder.Release()
							}()

							var testSpans roachpb.Spans
							for batch := source.Next(); ; batch = source.Next() {
								if batch.Length() == 0 {
									// Reached the end of the input.
									testSpans = append(testSpans, colBuilder.GetSpans()...)
									break
								}
								if useSel {
									batch.SetSelection(true)
									copy(batch.Selection(), sel)
									batch.SetLength(len(sel))
								}
								colBuilder.ConsumeBatch(batch, 0 /* startIdx */, batch.Length() /* endIdx */)
							}

							var oracleSpans roachpb.Spans
							for batch := oracleSource.Next(); batch.Length() > 0; batch = oracleSource.Next() {
								batch.SetSelection(true)
								copy(batch.Selection(), sel)
								batch.SetLength(len(sel))
								converter.ConvertBatchAndDeselect(batch)
								rows := make(rowenc.EncDatumRows, len(sel))
								for i := range sel {
									// Note that sel contains all rows if useSel=false.
									row := make(rowenc.EncDatumRow, len(typs))
									for j := range typs {
										datum := converter.GetDatumColumn(j)[i]
										row[j] = rowenc.DatumToEncDatum(typs[j], datum)
									}
									rows[i] = row
								}
								oracleSpans = append(oracleSpans, spanGeneratorOracle(t, &builder, splitter, rows, len(typs))...)
							}

							if len(oracleSpans) != len(testSpans) {
								t.Fatalf("Expected %d spans, got %d.", len(oracleSpans), len(testSpans))
							}
							for i := range oracleSpans {
								oracleSpan := oracleSpans[i]
								testSpan := testSpans[i]
								if !reflect.DeepEqual(oracleSpan, testSpan) {
									t.Fatalf("Span at index %d incorrect.\n\nExpected:\n%v\n\nFound:\n%v\n",
										i, oracleSpan, testSpan)
								}
							}
						})
					}
				})
			}
		})
	}
}

// spanGeneratorOracle extracts the logic from joinreader_span_generator.go that
// pertains to index joins.
func spanGeneratorOracle(
	t *testing.T,
	spanBuilder *span.Builder,
	spanSplitter span.Splitter,
	rows []rowenc.EncDatumRow,
	lookupCols int,
) roachpb.Spans {
	var spans roachpb.Spans
	for _, inputRow := range rows {
		generatedSpan, containsNull, err := spanBuilder.SpanFromEncDatums(inputRow[:lookupCols])
		if err != nil {
			t.Fatal(err)
		}
		spans = spanSplitter.MaybeSplitSpanIntoSeparateFamilies(
			spans, generatedSpan, lookupCols, containsNull)
	}
	return spans
}

func makeTable(useColFamilies bool) catalog.TableDescriptor {
	tableID := bootstrap.TestingUserDescID(0)
	if !useColFamilies {
		// We can prevent the span builder from splitting spans into separate column
		// families by using a system table ID, since system tables do not have
		// column families.
		tableID = keys.SystemDatabaseID
	}

	var testTableDesc = descpb.TableDescriptor{
		Name:       "abcd",
		ID:         descpb.ID(tableID),
		Privileges: catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
		Version:    1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "a", ID: 1, Type: types.Int},
			{Name: "b", ID: 2, Type: types.Bytes},
			{Name: "c", ID: 3, Type: types.Decimal},
			{Name: "d", ID: 4, Type: types.Int},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{
			{Name: "primary", ID: 0, ColumnNames: []string{"a", "b", "d"}, ColumnIDs: []descpb.ColumnID{1, 2, 4}},
			{Name: "secondary", ID: 1, ColumnNames: []string{"c"}, ColumnIDs: []descpb.ColumnID{3}},
		},
		NextFamilyID: 2,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"a", "b", "c"},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1, 2, 3},
		},
		Indexes: []descpb.IndexDescriptor{
			{ // Secondary index omits column 'd'.
				Name:                "secondary",
				ID:                  2,
				Unique:              true,
				KeyColumnNames:      []string{"c", "a", "b"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				KeyColumnIDs:        []descpb.ColumnID{3, 1, 2},
				KeySuffixColumnIDs:  []descpb.ColumnID{1, 2},
			},
		},
		NextIndexID:    3,
		FormatVersion:  descpb.FamilyFormatVersion,
		NextMutationID: 1,
	}

	b := tabledesc.NewBuilder(&testTableDesc)
	b.RunPostDeserializationChanges()
	return b.BuildImmutableTable()
}
