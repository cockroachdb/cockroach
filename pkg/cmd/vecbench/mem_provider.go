// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/memstore"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

const seed = 42

// MemProvider implements VectorProvider using an in-memory store.
type MemProvider struct {
	stopper          *stop.Stopper
	indexFileName    string
	dims             int
	options          cspann.IndexOptions
	store            *memstore.Store
	index            *cspann.Index
	successfulSplits atomic.Int64
}

// NewMemProvider creates a new MemProvider that maintains an in-memory, indexed
// dataset of vectors.
func NewMemProvider(
	stopper *stop.Stopper, datasetName string, dims int, options cspann.IndexOptions,
) *MemProvider {
	indexFileName := fmt.Sprintf("%s/%s.idx", tempDir, datasetName)
	return &MemProvider{
		stopper:       stopper,
		indexFileName: indexFileName,
		dims:          dims,
		options:       options,
	}
}

// Close implements the VectorProvider interface.
func (m *MemProvider) Close() {
	if m.index != nil {
		m.index.Close()
	}
	m.store = nil
	m.index = nil
	m.successfulSplits.Store(0)
}

// Load implements the VectorProvider interface.
func (m *MemProvider) Load(ctx context.Context) (bool, error) {
	// If no index file exists, return false.
	_, err := os.Stat(m.indexFileName)
	if err != nil {
		if oserror.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	// Clear any in-memory vectors (but not index file).
	m.Close()

	// Load vectors from the index file.
	m.store, err = loadMemStore(m.indexFileName)
	if err != nil {
		return false, err
	}
	if m.store.Dims() != m.dims {
		return false, errors.AssertionFailedf(
			"expected index with %d dims, got %d", m.dims, m.store.Dims())
	}

	if err = m.ensureIndex(ctx); err != nil {
		return false, err
	}

	return true, nil
}

// New implements the VectorProvider interface
func (m *MemProvider) New(ctx context.Context) error {
	// Clear any existing state.
	m.Close()

	// Remove persisted index, if it exists.
	err := os.Remove(m.indexFileName)
	if err != nil {
		if !oserror.IsNotExist(err) {
			return err
		}
	}

	return m.ensureIndex(ctx)
}

// InsertVector implements the VectorProvider interface.
func (m *MemProvider) InsertVectors(
	ctx context.Context, keys []cspann.KeyBytes, vectors vector.Set,
) (err error) {
	return m.store.RunTransaction(ctx, func(txn cspann.Txn) error {
		var idxCtx cspann.Context
		idxCtx.Init(txn)
		for i := range vectors.Count {
			key := keys[i]
			vec := vectors.At(i)
			m.store.InsertVector(key, vec)
			if err = m.index.Insert(ctx, &idxCtx, nil /* treeKey */, vec, key); err != nil {
				return err
			}
		}
		return nil
	})
}

// Search implements the VectorProvider interface.
func (m *MemProvider) Search(
	ctx context.Context, vec vector.T, maxResults int, beamSize int, stats *cspann.SearchStats,
) (keys []cspann.KeyBytes, err error) {
	err = m.store.RunTransaction(ctx, func(txn cspann.Txn) error {
		// Search the store.
		var idxCtx cspann.Context
		idxCtx.Init(txn)
		searchSet := cspann.SearchSet{MaxResults: maxResults}
		searchOptions := cspann.SearchOptions{BaseBeamSize: beamSize}
		err = m.index.Search(ctx, &idxCtx, nil /* treeKey */, vec, &searchSet, searchOptions)
		if err != nil {
			return err
		}
		*stats = searchSet.Stats

		// Get result keys.
		results := searchSet.PopResults()
		keys = make([]cspann.KeyBytes, len(results))
		for i, res := range results {
			keys[i] = []byte(res.ChildKey.KeyBytes)
		}

		return nil
	})

	return keys, err
}

// Save implements the VectorProvider interface.
func (m *MemProvider) Save(ctx context.Context) error {
	if m.index == nil {
		// Nothing to do.
		return nil
	}

	// Wait for any remaining background fixups to be processed.
	m.index.ProcessFixups()

	startTime := timeutil.Now()

	indexBytes, err := m.store.MarshalBinary()
	if err != nil {
		return err
	}

	indexFile, err := os.Create(m.indexFileName)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	_, err = indexFile.Write(indexBytes)
	if err != nil {
		return err
	}

	elapsed := timeutil.Since(startTime)
	fmt.Printf(Cyan+"Saved index to disk in %v\n"+Reset, roundDuration(elapsed))

	return nil
}

// GetMetrics implements the VectorProvider interface.
func (m *MemProvider) GetMetrics() ([]IndexMetric, error) {
	// successfulSplits is the number of splits successfuly completed by the
	// background fixup processor.
	successfulSplits := IndexMetric{Name: "successful splits"}

	// pendingSplitsMerges is the number of splits/merges waiting to be processed
	// by the background fixup processor.
	pendingSplitsMerges := IndexMetric{Name: "pending splits/merges"}

	// pacerOpsPerSec returnss the ops/sec currently allowed by the pacer.
	pacerOpsPerSec := IndexMetric{Name: "pacer ops/sec"}

	if m.index != nil {
		successfulSplits.Value = float64(m.successfulSplits.Load())
		pendingSplitsMerges.Value = float64(m.index.Fixups().PendingSplitsMerges())
		pacerOpsPerSec.Value = m.index.Fixups().AllowedOpsPerSec()
	}

	return []IndexMetric{successfulSplits, pendingSplitsMerges, pacerOpsPerSec}, nil
}

// FormatStats implements the VectorProvider interface.
func (m *MemProvider) FormatStats() string {
	if m.index == nil {
		return ""
	}
	return m.index.FormatStats()
}

// ensureIndex constructs an in-memory store and index if one hasn't yet been
// created.
func (m *MemProvider) ensureIndex(ctx context.Context) error {
	if m.index != nil {
		return nil
	}

	quantizer := quantize.NewRaBitQuantizer(m.dims, seed)
	if m.store == nil {
		// Construct empty store if one doesn't yet exist.
		m.store = memstore.New(quantizer, seed)
	}

	var err error
	m.index, err = cspann.NewIndex(ctx, m.store, quantizer, seed, &m.options, m.stopper)
	m.index.Fixups().OnSuccessfulSplit(func() {
		m.successfulSplits.Add(1)
	})
	return err
}

// loadMemStore loads a previously saved in-memory store from disk.
func loadMemStore(fileName string) (*memstore.Store, error) {
	startTime := timeutil.Now()

	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	memStore, err := memstore.Load(data)
	if err != nil {
		return nil, err
	}

	elapsed := timeutil.Since(startTime)
	fmt.Printf(Cyan+"Loaded %s index from disk in %v\n"+Reset, fileName, roundDuration(elapsed))

	return memStore, nil
}
