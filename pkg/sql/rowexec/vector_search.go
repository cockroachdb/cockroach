// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

type vectorSearchProcessor struct {
	execinfra.ProcessorBase
	fetchSpec   *fetchpb.IndexFetchSpec
	colOrdMap   catalog.TableColMap
	prefixKey   roachpb.Key
	queryVector vector.T

	idx         *cspann.Index
	idxCtx      cspann.Context
	targetCount uint64
	searchDone  bool

	row     rowenc.EncDatumRow
	scratch rowenc.EncDatumRow
	res     cspann.SearchResults
	resIdx  int
}

var _ execinfra.RowSourcedProcessor = &vectorSearchProcessor{}
var _ execopnode.OpNode = &vectorSearchProcessor{}

func newVectorSearchProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.VectorSearchSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	if spec.PrefixKey != nil {
		return nil, unimplemented.New("prefix columns",
			"searching a vector index with prefix columns is not yet supported")
	}
	idx, vecTxn, err := getVectorIndexForSearch(ctx, flowCtx, &spec.FetchSpec)
	if err != nil {
		return nil, err
	}
	v := vectorSearchProcessor{
		fetchSpec:   &spec.FetchSpec,
		prefixKey:   spec.PrefixKey,
		queryVector: spec.QueryVector,
		idx:         idx,
		targetCount: spec.TargetNeighborCount,
	}
	v.idxCtx.Init(vecTxn)
	colTypes := make([]*types.T, len(v.fetchSpec.FetchedColumns))
	for i, col := range v.fetchSpec.FetchedColumns {
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
		if err = v.search(); err != nil {
			v.MoveToDraining(err)
		}
	}
	for v.State == execinfra.StateRunning {
		if v.resIdx >= len(v.res) {
			v.MoveToDraining(nil /* err */)
			break
		}
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

func (v *vectorSearchProcessor) search() error {
	// An index-join + top-k operation will handle the re-ranking later, so we
	// skip doing it in the vector-search operator.
	searchOptions := cspann.SearchOptions{SkipRerank: true}
	searchSet := cspann.SearchSet{MaxResults: int(v.targetCount)}
	err := v.idx.Search(
		v.Ctx(), &v.idxCtx, cspann.TreeKey(v.prefixKey), v.queryVector, &searchSet, searchOptions)
	if err != nil {
		return err
	}
	v.res = searchSet.PopUnsortedResults()
	return nil
}

// processSearchResult decodes the primary key columns from the search result
// and stores them in v.vals.
func (v *vectorSearchProcessor) processSearchResult(res cspann.SearchResult) (err error) {
	if v.row == nil {
		v.row = make(rowenc.EncDatumRow, len(v.fetchSpec.FetchedColumns))
	}
	decodeFromKey := func(keyCols []fetchpb.IndexFetchSpec_KeyColumn, keyBytes []byte) error {
		if len(keyCols) == 0 {
			if len(keyBytes) > 0 {
				return errors.AssertionFailedf("expected empty key bytes")
			}
			return nil
		}
		if cap(v.scratch) < len(keyCols) {
			v.scratch = make(rowenc.EncDatumRow, len(keyCols))
		}
		v.scratch = v.scratch[:len(keyCols)]
		_, _, err = rowenc.DecodeKeyValsUsingSpec(keyCols, keyBytes, v.scratch)
		if err != nil {
			return err
		}
		for i, col := range keyCols {
			idx, ok := v.colOrdMap.Get(col.ColumnID)
			if !ok {
				continue
			}
			v.row[idx] = v.scratch[i]
		}
		return nil
	}
	// Decode the index prefix columns, if any,
	keyPrefixCols := v.fetchSpec.KeyColumns()[:len(v.fetchSpec.KeyColumns())-1]
	if err = decodeFromKey(keyPrefixCols, v.prefixKey); err != nil {
		return err
	}
	// Decode the index suffix columns. This is usually the set of primary key
	// columns.
	keySuffixCols, keySuffixBytes := v.fetchSpec.KeySuffixColumns(), res.ChildKey.KeyBytes
	if err = decodeFromKey(keySuffixCols, keySuffixBytes); err != nil {
		return err
	}
	neededValueCols := len(v.fetchSpec.FetchedColumns)
	_, err = rowenc.DecodeValueBytes(v.colOrdMap, res.ValueBytes, neededValueCols, v.row)
	return err
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
	input      execinfra.RowSource
	datumAlloc tree.DatumAlloc

	prefixKeyColOrds  []uint32
	prefixKeyCols     []fetchpb.IndexFetchSpec_KeyColumn
	queryVectorColOrd int
	suffixKeyColOrds  []uint32
	suffixKeyCols     []fetchpb.IndexFetchSpec_KeyColumn
	scratchDatums     tree.Datums

	idx    *cspann.Index
	idxCtx cspann.Context

	isIndexPut bool
}

var _ execinfra.RowSourcedProcessor = &vectorMutationSearchProcessor{}
var _ execopnode.OpNode = &vectorMutationSearchProcessor{}

func newVectorMutationSearchProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.VectorMutationSearchSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	idx, vecTxn, err := getVectorIndexForSearch(ctx, flowCtx, &spec.FetchSpec)
	if err != nil {
		return nil, err
	}
	v := vectorMutationSearchProcessor{
		input:             input,
		prefixKeyColOrds:  spec.PrefixKeyColumnOrdinals,
		prefixKeyCols:     spec.PrefixKeyColumns,
		queryVectorColOrd: int(spec.QueryVectorColumnOrdinal),
		suffixKeyColOrds:  spec.PrefixKeyColumnOrdinals,
		suffixKeyCols:     spec.SuffixKeyColumns,
		isIndexPut:        spec.IsIndexPut,
		idx:               idx,
	}
	v.idxCtx.Init(vecTxn)

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
		prefix, err := v.encodePrefixKeyCols(row)
		if err != nil {
			v.MoveToDraining(err)
			break
		}
		if row[v.queryVectorColOrd].Datum != tree.DNull {
			queryVector := tree.MustBeDPGVector(row[v.queryVectorColOrd].Datum).T
			if v.isIndexPut {
				searchRes, err := v.idx.SearchForInsert(
					v.Ctx(), &v.idxCtx, cspann.TreeKey(prefix), queryVector)
				if err != nil {
					v.MoveToDraining(err)
					break
				}
				partition := searchRes.ChildKey.PartitionKey
				partitionKey = tree.NewDInt(tree.DInt(partition))
				partitionCentroid := searchRes.Vector
				quantizedVec, err = v.quantize(partition, partitionCentroid, queryVector)
				if err != nil {
					v.MoveToDraining(err)
					break
				}
			} else {
				pk, err := v.encodeSuffixKeyCols(row)
				if err != nil {
					v.MoveToDraining(err)
					break
				}
				searchRes, err := v.idx.SearchForDelete(
					v.Ctx(), &v.idxCtx, cspann.TreeKey(prefix), queryVector, pk)
				if err != nil {
					v.MoveToDraining(err)
					break
				}
				// It is possible for the search not to find the target index entry, in
				// which case the result is nil.
				if searchRes != nil {
					partitionKey = tree.NewDInt(tree.DInt(searchRes.ParentPartitionKey))
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

func (v *vectorMutationSearchProcessor) quantize(
	partition cspann.PartitionKey, centroid, queryVec vector.T,
) (tree.Datum, error) {
	randomizedVec := make(vector.T, len(queryVec))
	randomizedVec = v.idx.RandomizeVector(queryVec, randomizedVec)
	store := v.idx.Store().(*vecstore.Store)
	quantizedVec, err := store.QuantizeAndEncode(partition, centroid, randomizedVec)
	if err != nil {
		return nil, err
	}
	return tree.NewDBytes(tree.DBytes(quantizedVec)), nil
}

func (v *vectorMutationSearchProcessor) encodePrefixKeyCols(
	row rowenc.EncDatumRow,
) (prefix roachpb.Key, err error) {
	if len(v.prefixKeyColOrds) == 0 {
		return nil, nil
	}
	return v.encodePartialKey(row, v.prefixKeyColOrds, v.prefixKeyCols)
}

func (v *vectorMutationSearchProcessor) encodeSuffixKeyCols(
	row rowenc.EncDatumRow,
) (cspann.KeyBytes, error) {
	if len(v.suffixKeyColOrds) == 0 {
		return nil, errors.AssertionFailedf("expected non-empty suffix key columns")
	}
	if len(v.suffixKeyCols) != len(v.suffixKeyColOrds) {
		return nil, errors.AssertionFailedf("expected suffix column and ord lists to match")
	}
	suffix, err := v.encodePartialKey(row, v.suffixKeyColOrds, v.suffixKeyCols)
	if err != nil {
		return nil, err
	}
	// Add the sentinel column family value to match the index encoding exactly.
	return keys.MakeFamilyKey(suffix, 0), nil
}

func (v *vectorMutationSearchProcessor) encodePartialKey(
	row rowenc.EncDatumRow, keyColOrds []uint32, keyCols []fetchpb.IndexFetchSpec_KeyColumn,
) (roachpb.Key, error) {
	if cap(v.scratchDatums) < len(keyColOrds) {
		v.scratchDatums = make(tree.Datums, len(keyColOrds))
	}
	vals := v.scratchDatums[:len(keyColOrds)]
	for i, colOrd := range keyColOrds {
		if err := row[colOrd].EnsureDecoded(keyCols[i].Type, &v.datumAlloc); err != nil {
			v.MoveToDraining(err)
			break
		}
		vals[i] = row[colOrd].Datum
	}
	var colOrdMap catalog.TableColMap
	for i, col := range keyCols {
		colOrdMap.Set(col.ColumnID, i)
	}
	var prefix []byte
	key, _, err := rowenc.EncodePartialIndexKey(keyCols, colOrdMap, vals, prefix)
	return key, err
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

func getVectorIndexForSearch(
	ctx context.Context, flowCtx *execinfra.FlowCtx, fetchSpec *fetchpb.IndexFetchSpec,
) (*cspann.Index, cspann.Txn, error) {
	idxManager := flowCtx.Cfg.VecIndexManager.(*vecindex.Manager)
	idx, err := idxManager.Get(ctx, fetchSpec.TableID, fetchSpec.IndexID)
	if err != nil {
		return nil, nil, err
	}
	vecTxn := idx.Store().(*vecstore.Store).WrapTxn(flowCtx.Txn)
	return idx, vecTxn, nil
}
