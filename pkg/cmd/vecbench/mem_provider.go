// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"

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
	stopper       *stop.Stopper
	indexFileName string
	dims          int
	options       cspann.IndexOptions
	store         *memstore.Store
	index         *cspann.Index
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

	return true, nil
}

// Clear implements the VectorProvider interface
func (m *MemProvider) Clear(ctx context.Context) error {
	m.Close()
	err := os.Remove(m.indexFileName)
	if oserror.IsNotExist(err) {
		return nil
	}
	return err
}

// InsertVector implements the VectorProvider interface.
func (m *MemProvider) InsertVectors(
	ctx context.Context, keys []cspann.KeyBytes, vectors vector.Set,
) (err error) {
	if err = m.ensureIndex(ctx); err != nil {
		return err
	}

	var txn cspann.Txn
	txn, err = m.store.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = m.store.CommitTransaction(ctx, txn)
		}
		if err != nil {
			err = errors.CombineErrors(err, m.store.AbortTransaction(ctx, txn))
		}
	}()

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

	return err
}

// Search implements the VectorProvider interface.
func (m *MemProvider) Search(
	ctx context.Context, vec vector.T, maxResults int, beamSize int, stats *cspann.SearchStats,
) (keys []cspann.KeyBytes, err error) {
	if err = m.ensureIndex(ctx); err != nil {
		return nil, err
	}

	var txn cspann.Txn
	txn, err = m.store.BeginTransaction(ctx)
	defer func() {
		if err == nil {
			err = m.store.CommitTransaction(ctx, txn)
		}
		if err != nil {
			err = m.store.AbortTransaction(ctx, txn)
		}
	}()

	// Search the store.
	var idxCtx cspann.Context
	idxCtx.Init(txn)
	searchSet := cspann.SearchSet{MaxResults: maxResults}
	searchOptions := cspann.SearchOptions{BaseBeamSize: beamSize}
	err = m.index.Search(ctx, &idxCtx, nil /* treeKey */, vec, &searchSet, searchOptions)
	if err != nil {
		return nil, err
	}
	*stats = searchSet.Stats

	// Get result keys.
	results := searchSet.PopResults()
	keys = make([]cspann.KeyBytes, len(results))
	for i, res := range results {
		keys[i] = []byte(res.ChildKey.KeyBytes)
	}

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
func (m *MemProvider) GetMetrics() []IndexMetric {
	// queueSize is the size of the background fixup queue for processing splits
	// and merges.
	queueSize := IndexMetric{Name: "fixup queue size"}

	// pacerOpsPerSec returnss the ops/sec currently allowed by the pacer.
	pacerOpsPerSec := IndexMetric{Name: "pacer ops/sec"}

	if m.index != nil {
		queueSize.Value = float64(m.index.Fixups().QueueSize())
		pacerOpsPerSec.Value = m.index.Fixups().AllowedOpsPerSec()
	}

	return []IndexMetric{queueSize, pacerOpsPerSec}
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
	if m.store == nil {
		// Construct empty store if one doesn't yet exist.
		m.store = memstore.New(m.dims, seed)
	}

	var err error
	quantizer := quantize.NewRaBitQuantizer(m.dims, seed)
	m.index, err = cspann.NewIndex(ctx, m.store, quantizer, seed, &m.options, m.stopper)
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
