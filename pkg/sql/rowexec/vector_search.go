// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// vectorSearchHelper encapsulates the common logic for SQL operators that
// search within a vector index.
type vectorSearchHelper struct {
	ctx     context.Context
	idx     *vecindex.VectorIndex
	vecTxn  vecstore.Txn
	options vecindex.SearchOptions
	level   vecstore.Level
	target  int
}

func (h *vectorSearchHelper) init(
	ctx context.Context,
	txn *kv.Txn,
	idx *vecindex.VectorIndex,
	options vecindex.SearchOptions,
	level vecstore.Level,
	targetNeighborCount uint64,
) {
	*h = vectorSearchHelper{
		ctx:     ctx,
		idx:     idx,
		vecTxn:  idx.Store().(*vecstore.PersistentStore).WrapTxn(txn),
		options: options,
		level:   level,
		target:  int(targetNeighborCount),
	}
}

func (h *vectorSearchHelper) search(
	vec vector.T, primaryKey vecstore.PrimaryKey,
) (vecstore.SearchResults, error) {
	searchSet := vecstore.SearchSet{MaxResults: h.target, MatchKey: primaryKey}
	err := h.idx.Search(h.ctx, h.vecTxn, vec, &searchSet, h.options, h.level)
	if err != nil {
		return nil, err
	}
	return searchSet.PopUnsortedResults(), nil
}

func (h *vectorSearchHelper) quantize(
	partition vecstore.PartitionKey, centroid, queryVec vector.T,
) (tree.Datum, error) {
	store := h.idx.Store().(*vecstore.PersistentStore)
	quantizedVec, err := store.QuantizeAndEncode(h.ctx, partition, centroid, queryVec)
	if err != nil {
		return nil, err
	}
	return tree.NewDBytes(tree.DBytes(quantizedVec)), nil
}

type vectorSearchProcessor struct {
	execinfra.ProcessorBase
	pkCols      []fetchpb.IndexFetchSpec_KeyColumn
	colOrdMap   catalog.TableColMap
	queryVector vector.T

	helper     vectorSearchHelper
	searchDone bool

	row    rowenc.EncDatumRow
	res    vecstore.SearchResults
	resIdx int
}

var _ execinfra.Processor = &vectorSearchProcessor{}
var _ execinfra.RowSource = &vectorSearchProcessor{}
var _ execopnode.OpNode = &vectorSearchProcessor{}

func newVectorSearchProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.VectorSearchSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	// TODO(drewk): retrieve the VectorIndex from the manager.
	var idx *vecindex.VectorIndex
	v := vectorSearchProcessor{
		queryVector: spec.QueryVector,
	}
	colTypes := make([]*types.T, len(spec.PrimaryKeyColumns))
	for i, col := range spec.PrimaryKeyColumns {
		colTypes[i] = col.Type
		v.colOrdMap.Set(col.ColumnID, i)
	}
	if err := v.Init(
		ctx,
		&v,
		post,
		colTypes,
		flowCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	// An index-join + top-k operation will handle the re-ranking later, so we
	// skip doing it in the vector-search operator.
	searchOptions := vecindex.SearchOptions{SkipRerank: true}
	v.helper.init(ctx, flowCtx.Txn, idx, searchOptions, vecstore.LeafLevel, spec.TargetNeighborCount)
	return &v, nil
}

// Start is part of the RowSource interface.
func (v *vectorSearchProcessor) Start(ctx context.Context) {
	v.StartInternal(ctx, "vector search")
}

// Next is part of the RowSource interface.
func (v *vectorSearchProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	var err error
	if !v.searchDone {
		v.searchDone = true
		v.res, err = v.helper.search(v.queryVector, nil /* primaryKey */)
		if err != nil {
			v.MoveToDraining(err)
			return nil, v.DrainHelper()
		}
	}
	for v.State == execinfra.StateRunning && v.resIdx >= len(v.res) {
		next := v.res[v.resIdx]
		v.resIdx++
		if err = v.processSearchResult(next); err != nil {
			v.MoveToDraining(err)
			break
		}
		if outRow := v.ProcessRowHelper(v.row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, v.DrainHelper()
}

// processSearchResult decodes the primary key columns from the search result
// and stores them in v.vals.
func (v *vectorSearchProcessor) processSearchResult(res vecstore.SearchResult) (err error) {
	encPK := res.ChildKey.PrimaryKey
	if v.row == nil {
		v.row = make(rowenc.EncDatumRow, len(v.pkCols))
	}
	_, _, err = rowenc.DecodeKeyValsUsingSpec(v.pkCols, encPK, v.row)
	if err != nil {
		return err
	}
	// TODO(drewk): the SearchResult should store the composite key column
	// encoding.
	var compositeValBytes []byte
	return v.decodeCompositeVals(compositeValBytes)
}

// decodeCompositeVals decodes the optional value encodings for composite-typed
// key columns. This ensures that the exact values are recovered for those
// columns. For example, the decimal '1.00' in the key would drop the trailing
// zeros, while the composite encoding would preserve them.
func (v *vectorSearchProcessor) decodeCompositeVals(compositeValBytes []byte) (err error) {
	var (
		colIDDiff  uint32
		lastColID  descpb.ColumnID
		typeOffset int
		dataOffset int
		typ        encoding.Type
	)
	// Continue reading data until there's none left, or we've finished populating
	// the data for all the requested composite-typed columns.
	for len(compositeValBytes) > 0 {
		typeOffset, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(compositeValBytes)
		if err != nil {
			return err
		}
		colID := lastColID + descpb.ColumnID(colIDDiff)
		lastColID = colID
		idx, ok := v.colOrdMap.Get(colID)
		if !ok {
			// This column wasn't requested, so read its length and skip it.
			numBytes, err := encoding.PeekValueLengthWithOffsetsAndType(compositeValBytes, dataOffset, typ)
			if err != nil {
				return err
			}
			compositeValBytes = compositeValBytes[numBytes:]
			continue
		}
		var encValue rowenc.EncDatum
		encValue, compositeValBytes, err = rowenc.EncDatumValueFromBufferWithOffsetsAndType(
			compositeValBytes, typeOffset, dataOffset, typ,
		)
		if err != nil {
			return err
		}
		v.row[idx] = encValue
	}
	return nil
}

// ChildCount is part of the execopnode.OpNode interface.
func (v *vectorSearchProcessor) ChildCount(verbose bool) int {
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (v *vectorSearchProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

type vectorMutationSearchProcessor struct {
	execinfra.ProcessorBase
	idx        *vecindex.VectorIndex
	store      *vecstore.PersistentStore
	input      execinfra.RowSource
	datumAlloc tree.DatumAlloc

	queryVectorColOrd int
	primaryKeyColOrds []int
	primaryKeyCols    []fetchpb.IndexFetchSpec_KeyColumn
	pkValScratch      tree.Datums

	helper vectorSearchHelper

	isIndexPut bool
}

var _ execinfra.Processor = &vectorMutationSearchProcessor{}
var _ execinfra.RowSource = &vectorMutationSearchProcessor{}
var _ execopnode.OpNode = &vectorMutationSearchProcessor{}

func newVectorMutationSearchProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.VectorMutationSearchSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	// TODO(drewk): retrieve the VectorIndex from the manager.
	var idx *vecindex.VectorIndex
	v := vectorMutationSearchProcessor{
		input:      input,
		isIndexPut: spec.IsIndexPut,
	}
	// Pass through the input columns, and add the partition column and optional
	// quantized vector column.
	outputTypes := make([]*types.T, len(input.OutputTypes()), len(input.OutputTypes())+2)
	copy(outputTypes, input.OutputTypes())
	outputTypes = append(outputTypes, types.Int)
	if v.isIndexPut {
		outputTypes = append(outputTypes, types.Bytes)
	}
	if err := v.Init(
		ctx,
		&v,
		post,
		outputTypes,
		flowCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{v.input},
		},
	); err != nil {
		return nil, err
	}
	const targetNeighborCount = 1
	var level vecstore.Level
	if v.isIndexPut {
		// For an index put, we're looking for the best partition to insert into.
		// We don't need to look at any particular vector in the leaf level.
		level = vecstore.SecondLevel
	} else {
		// For an index del, we're looking for the leaf-level index entry
		// corresponding to a particular row, identified by its primary key.
		level = vecstore.LeafLevel
	}
	// ReturnVectors is true for index puts so that the centroids are returned,
	// which is needed for quantizing the input vectors.
	searchOptions := vecindex.SearchOptions{ReturnVectors: v.isIndexPut}
	v.helper.init(ctx, flowCtx.Txn, idx, searchOptions, level, targetNeighborCount)
	return &v, nil
}

// Start is part of the RowSource interface.
func (v *vectorMutationSearchProcessor) Start(ctx context.Context) {
	ctx = v.StartInternal(ctx, "vector mutation search")
	v.input.Start(ctx)
}

// Next is part of the RowSource interface.
func (v *vectorMutationSearchProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for v.State == execinfra.StateRunning {
		row, meta := v.input.Next()

		if meta != nil {
			if meta.Err != nil {
				v.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			v.MoveToDraining(nil /* err */)
			break
		}
		if err := row[v.queryVectorColOrd].EnsureDecoded(types.PGVector, &v.datumAlloc); err != nil {
			v.MoveToDraining(err)
			break
		}
		// The output values are NULL if the input query vector is NULL. For an
		// index del, the search can fail to find the target index entry, in which
		// case the output is also NULL.
		var (
			partitionKey = tree.DNull
			quantizedVec = tree.DNull
		)
		if row[v.queryVectorColOrd].Datum != tree.DNull {
			var err error
			var pk vecstore.PrimaryKey
			if !v.isIndexPut {
				pk, err = v.encodePrimaryKeyCols(row)
				if err != nil {
					v.MoveToDraining(err)
					break
				}
			}
			queryVector := tree.MustBeDPGVector(row[v.queryVectorColOrd].Datum).T
			res, err := v.searchAndValidate(queryVector, pk)
			if err != nil {
				v.MoveToDraining(err)
				break
			}
			if res != nil {
				if v.isIndexPut {
					partition := res.ChildKey.PartitionKey
					partitionKey = tree.NewDInt(tree.DInt(partition))
					partitionCentroid := res.Vector
					quantizedVec, err = v.helper.quantize(partition, partitionCentroid, queryVector)
					if err != nil {
						v.MoveToDraining(err)
						break
					}
				} else {
					// For an index del, we searched for the leaf-level index entry
					// with matching primary key, so the partition to delete from is the
					// parent.
					partitionKey = tree.NewDInt(tree.DInt(res.ParentPartitionKey))
				}
			}
		}
		row = append(row, rowenc.DatumToEncDatum(types.Int, partitionKey))
		if v.isIndexPut {
			row = append(row, rowenc.DatumToEncDatum(types.Bytes, quantizedVec))
		}
		if outRow := v.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, v.DrainHelper()
}

func (v *vectorMutationSearchProcessor) encodePrimaryKeyCols(
	row rowenc.EncDatumRow,
) (pk vecstore.PrimaryKey, err error) {
	if len(v.primaryKeyColOrds) == 0 {
		return nil, errors.AssertionFailedf("expected non-empty primary key columns")
	}
	if v.pkValScratch == nil {
		v.pkValScratch = make(tree.Datums, len(v.primaryKeyColOrds))
	}
	pkVals := v.pkValScratch
	for i, colOrd := range v.primaryKeyColOrds {
		if err := row[colOrd].EnsureDecoded(v.primaryKeyCols[i].Type, &v.datumAlloc); err != nil {
			v.MoveToDraining(err)
			break
		}
		pkVals[i] = row[colOrd].Datum
	}
	var colOrdMap catalog.TableColMap
	for i, col := range v.primaryKeyCols {
		colOrdMap.Set(col.ColumnID, i)
	}
	var prefix []byte
	pk, _, err = rowenc.EncodePartialIndexKey(v.primaryKeyCols, colOrdMap, pkVals, prefix)
	return pk, err
}

func (v *vectorMutationSearchProcessor) searchAndValidate(
	vec vector.T, pk vecstore.PrimaryKey,
) (*vecstore.SearchResult, error) {
	res, err := v.helper.search(vec, pk)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	} else if len(res) > 1 {
		return nil, errors.AssertionFailedf("unexpected multiple search results")
	}
	if v.isIndexPut {
		if res[0].ChildKey.PartitionKey == vecstore.InvalidKey {
			return nil, errors.AssertionFailedf("unexpected invalid partition key")
		}
		if res[0].Vector == nil {
			return nil, errors.AssertionFailedf("missing partition centroid")
		}
	}
	return &res[0], nil
}

// ChildCount is part of the execopnode.OpNode interface.
func (v *vectorMutationSearchProcessor) ChildCount(verbose bool) int {
	if _, ok := v.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (v *vectorMutationSearchProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := v.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to vector mutation search is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
