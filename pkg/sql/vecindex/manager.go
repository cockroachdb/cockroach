// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
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
	ctx          context.Context
	stopper      *stop.Stopper
	codec        keys.SQLCodec
	db           descs.DB
	testingKnobs *VecIndexTestingKnobs
}

// NewManager returns a new vector index manager which maintains per-node vector
// index instances. We store a context for creating new vector index objects,
// since those outlive the context of Get calls.
func NewManager(
	ctx context.Context, stopper *stop.Stopper, codec keys.SQLCodec, db descs.DB,
) *Manager {
	mgr := &Manager{
		ctx:     ctx,
		stopper: stopper,
		codec:   codec,
		db:      db,
	}
	mgr.mu.indexes = make(map[indexKey]*indexEntry)

	return mgr
}

// indexKey uniquely identifies an index within the cluster.
type indexKey struct {
	tableID catid.DescID
	indexID catid.IndexID
}

type indexEntry struct {
	idx *cspann.Index
	// If mustWait is true, we are in the process of fetching the config and
	// starting the index. Other callers can wait on the waitCond until this
	// is false.
	mustWait bool
	waitCond sync.Cond
	err      error
}

// SetTestingKnobs sets the testing knobs for the manager to use in unit tests.
func (m *Manager) SetTestingKnobs(knobs *VecIndexTestingKnobs) {
	m.testingKnobs = knobs
}

// Get returns the vector index for the given DB table and index. If the DB
// index does not currently have an active vector index, one is created and
// cached.
func (m *Manager) Get(
	ctx context.Context, tableID catid.DescID, indexID catid.IndexID,
) (*cspann.Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idxKey := indexKey{tableID, indexID}
	e := m.mu.indexes[idxKey]
	if e != nil {
		if e.mustWait {
			// We are in the process of grabbing the index config and starting the
			// vector index. Wait until that is complete, at which point e.idx will
			// be populated.
			log.VEventf(ctx, 1, "waiting for config for index %d of table %d", indexID, tableID)
			if m.testingKnobs != nil && m.testingKnobs.BeforeVecIndexWait != nil {
				m.testingKnobs.BeforeVecIndexWait()
			}
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

	idx, err := func() (*cspann.Index, error) {
		// Unlock while we build the index structure so that concurrent requests can be
		// serviced. We've already set mustWait to true, so other requests will wait
		// until we're done setting up the index.
		m.mu.Unlock()
		defer m.mu.Lock()
		if m.testingKnobs != nil && m.testingKnobs.DuringVecIndexPull != nil {
			m.testingKnobs.DuringVecIndexPull()
		}
		config, err := m.getVecConfig(ctx, tableID, indexID)
		if err != nil {
			return nil, err
		}
		// TODO(drewk): use the config to populate the index options as well.
		quantizer := quantize.NewRaBitQuantizer(int(config.Dims), config.Seed)
		store, err := vecstore.New(ctx, m.db, quantizer, m.codec, tableID, indexID)
		if err != nil {
			return nil, err
		}
		// Use the stored context so that the vector index can outlive the context
		// of the Get call. The fixup process gets a child context from the context
		// passed to cspann.NewIndex, and we don't want that to be the context of
		// the Get call.
		return cspann.NewIndex(m.ctx, store, quantizer, config.Seed, &cspann.IndexOptions{}, m.stopper)
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
) (vecpb.Config, error) {
	// Get the vector index config for the given table and index. Leased descriptors
	// are guaranteed to be physically compatible with the data on disk, even if a
	// schema change is in progress.
	var tableDesc catalog.TableDescriptor
	err := m.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		var err error
		tableDesc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, tableID)
		return err
	})
	if err != nil {
		return vecpb.Config{}, err
	}
	if tableDesc == nil {
		return vecpb.Config{}, errTableNotFound
	}
	var idxDesc catalog.Index
	for _, desc := range tableDesc.DeletableNonPrimaryIndexes() {
		if desc.GetID() == indexID {
			idxDesc = desc
			break
		}
	}
	if idxDesc == nil {
		return vecpb.Config{}, errIndexNotFound
	}
	config := idxDesc.GetVecConfig()
	if config.Dims <= 0 {
		return vecpb.Config{}, errInvalidVecConfig
	}
	return config, nil
}

var (
	errTableNotFound    = errors.New("table not found")
	errIndexNotFound    = errors.New("index not found")
	errInvalidVecConfig = errors.New("invalid vector index config")
)
