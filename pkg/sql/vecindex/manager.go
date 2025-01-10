// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Manager keeps track of the per-node state of each active vector index.
type Manager struct {
	mu struct {
		syncutil.Mutex
		// TODO(drewk): provide a way to remove inactive indexes from the map.
		indexes map[indexKey]*indexEntry
	}
	ctx     context.Context
	store   vecstore.Store
	stopper *stop.Stopper
	db      descs.DB
}

// NewManager returns a new vector index manager which maintains per-node
// VectorIndex instances.
func NewManager(
	ctx context.Context, store vecstore.Store, stopper *stop.Stopper, db descs.DB,
) *Manager {
	return &Manager{
		ctx:     ctx,
		store:   store,
		stopper: stopper,
		db:      db,
	}
}

// indexKey uniquely identifies an index within the cluster.
type indexKey struct {
	tableID catid.DescID
	indexID catid.IndexID
}

type indexEntry struct {
	idx *VectorIndex
	// If mustWait is true, we are in the process of fetching the config and
	// starting the VectorIndex. Other callers can wait on the waitCond until this
	// is false.
	mustWait bool
	waitCond sync.Cond
	err      error
}

// Get returns the VectorIndex for the given table and index. If the index does
// not currently have an active VectorIndex, one is created and cached.
func (m *Manager) Get(
	ctx context.Context, tableID catid.DescID, indexID catid.IndexID,
) (*VectorIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idxKey := indexKey{tableID, indexID}
	e := m.mu.indexes[idxKey]
	if e != nil {
		if e.mustWait {
			// We are in the process of grabbing the index config and starting the
			// VectorIndex. Wait until that is complete, at which point e.idx will be
			// populated.
			// TODO(drewk): should we check for context cancellation?
			log.VEventf(ctx, 1, "waiting for config for index %d of table %d", indexID, tableID)
			e.waitCond.Wait()
			log.VEventf(ctx, 1, "finished waiting for config for index %d of table %d", indexID, tableID)
		} else {
			// This is the expected "fast" path; don't emit an event.
			if log.V(2) {
				log.Infof(ctx, "config for index %d of table %d found in cache", indexID, tableID)
			}
		}
		return e.idx, e.err
	}
	e = &indexEntry{mustWait: true, waitCond: sync.Cond{L: &m.mu}}
	m.mu.indexes[idxKey] = e

	idx, err := func() (*VectorIndex, error) {
		m.mu.Unlock()
		defer m.mu.Lock()
		config, err := m.getVecConfig(ctx, tableID, indexID)
		if err != nil {
			return nil, err
		}
		// TODO(drewk): use the config to populate the index options as well.
		quantizer := quantize.NewRaBitQuantizer(int(config.Dims), config.Seed)
		return NewVectorIndex(m.ctx, m.store, quantizer, &VectorIndexOptions{}, m.stopper)
	}()
	e.mustWait = false
	e.idx, e.err = idx, err

	// Wake up any other callers that are waiting on these stats.
	e.waitCond.Broadcast()

	if err != nil {
		// Don't keep the index entry around, so that we retry the query.
		m.mu.indexes[idxKey] = nil
	}
	return idx, err
}

func (m *Manager) getVecConfig(
	ctx context.Context, tableID catid.DescID, indexID catid.IndexID,
) (vecstore.Config, error) {
	// TODO(drewk): Can we use the WithLeased variant?
	var tableDesc catalog.TableDescriptor
	err := m.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		var err error
		tableDesc, err = txn.Descriptors().ByIDWithoutLeased(txn.KV()).Get().Table(ctx, tableID)
		return err
	})
	if err != nil {
		return vecstore.Config{}, err
	}
	if tableDesc == nil {
		return vecstore.Config{}, errTableNotFound
	}
	var idxDesc catalog.Index
	for _, desc := range tableDesc.DeletableNonPrimaryIndexes() {
		if desc.GetID() == indexID {
			idxDesc = desc
			break
		}
	}
	if idxDesc == nil {
		return vecstore.Config{}, errIndexNotFound
	}
	config := idxDesc.GetVecConfig()
	if config.Dims <= 0 {
		return vecstore.Config{}, errInvalidVecConfig
	}
	return config, nil
}

var (
	errTableNotFound    = errors.New("table not found")
	errIndexNotFound    = errors.New("index not found")
	errInvalidVecConfig = errors.New("invalid vector index config")
)
