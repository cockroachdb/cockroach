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
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

type vectorSearchProcessor struct {
	execinfra.ProcessorBase
	fetchSpec   *fetchpb.IndexFetchSpec
	colOrdMap   catalog.TableColMap
	prefixKeys  []roachpb.Key
	queryVector vector.T

	searcher    vecindex.Searcher
	searchIdx   int
	currPrefix  roachpb.Key
	targetCount uint64

	row     rowenc.EncDatumRow
	scratch rowenc.EncDatumRow
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
	v := vectorSearchProcessor{
		fetchSpec:   &spec.FetchSpec,
		prefixKeys:  spec.PrefixKeys,
		queryVector: spec.QueryVector,
		targetCount: spec.TargetNeighborCount,
	}
	idx, err := getVectorIndexForSearch(ctx, flowCtx, &spec.FetchSpec)
	if err != nil {
		return nil, err
	}
	v.searcher.Init(idx, flowCtx.Txn)
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
	for v.State == execinfra.StateRunning {
		next := v.searcher.NextResult()
		if next == nil {
			// Either we haven't searched yet, or we have exhausted the current
			// search results.
			ok, err := v.maybeSearch()
			if !ok || err != nil {
				v.MoveToDraining(err)
				break
			}
			continue
		}
		if err := v.processSearchResult(next); err != nil {
			v.MoveToDraining(err)
			break
		}
		if outRow := v.ProcessRowHelper(v.row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, v.DrainHelper()
}

// maybeSearch performs the next vector search operation. It should be called
// when there are no further search results to process. It returns ok=false if
// there are no more searches to perform.
func (v *vectorSearchProcessor) maybeSearch() (ok bool, err error) {
	if v.searchIdx > 0 && v.searchIdx >= len(v.prefixKeys) {
		// We have conducted at least one search. If there are prefix keys, we have
		// already exhausted all of them. Note that there can be more than one
		// prefix key; this is useful for hash-sharded indexes and queries like the
		// following:
		//
		//   SELECT * FROM t@id_v_vector_idx
		//   WHERE id IN (100, 200, 300)
		//   ORDER BY v <-> '[1,2]' LIMIT 1;
		//
		return false, nil
	}
	if len(v.prefixKeys) > 0 {
		v.currPrefix = v.prefixKeys[v.searchIdx]
	}
	v.searchIdx++
	err = v.searcher.Search(v.Ctx(), v.currPrefix, v.queryVector, int(v.targetCount))
	if err != nil {
		return false, err
	}
	return true, nil
}

// processSearchResult decodes the primary key columns from the search result
// and stores them in v.vals.
func (v *vectorSearchProcessor) processSearchResult(res *cspann.SearchResult) (err error) {
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
	if err = decodeFromKey(keyPrefixCols, v.currPrefix); err != nil {
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

	searcher   vecindex.MutationSearcher
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
	v := vectorMutationSearchProcessor{
		input:             input,
		prefixKeyColOrds:  spec.PrefixKeyColumnOrdinals,
		prefixKeyCols:     spec.PrefixKeyColumns,
		queryVectorColOrd: int(spec.QueryVectorColumnOrdinal),
		suffixKeyColOrds:  spec.SuffixKeyColumnOrdinals,
		suffixKeyCols:     spec.SuffixKeyColumns,
		isIndexPut:        spec.IsIndexPut,
	}
	idx, err := getVectorIndexForSearch(ctx, flowCtx, &spec.FetchSpec)
	if err != nil {
		return nil, err
	}
	v.searcher.Init(idx, flowCtx.Txn)

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
				err = v.searcher.SearchForInsert(v.Ctx(), prefix, queryVector)
				if err != nil {
					v.MoveToDraining(err)
					break
				}
				partitionKey = v.searcher.PartitionKey()
				quantizedVec = v.searcher.EncodedVector()
			} else {
				pk, err := v.encodeSuffixKeyCols(row)
				if err != nil {
					v.MoveToDraining(err)
					break
				}
				err = v.searcher.SearchForDelete(v.Ctx(), prefix, queryVector, pk)
				if err != nil {
					v.MoveToDraining(err)
					break
				}
				// It is possible for the search not to find the target index entry, in
				// which case the result is nil.
				partitionKey = v.searcher.PartitionKey()
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
) (*cspann.Index, error) {
	idxManager := flowCtx.Cfg.VecIndexManager.(*vecindex.Manager)
	return idxManager.Get(ctx, fetchSpec.TableID, fetchSpec.IndexID)
}
