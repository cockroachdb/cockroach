// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// NewInMem allocates and returns a new, opened in-memory engine. The caller
// must call the engine's Close method when the engine is no longer needed.
//
// FIXME(tschottdorf): make the signature similar to NewRocksDB (require a cfg).
func NewInMem(engine enginepb.EngineType, attrs roachpb.Attributes, cacheSize int64) Engine {
	switch engine {
	case enginepb.EngineTypePebble:
		return newPebbleInMem(attrs, cacheSize)
	case enginepb.EngineTypeRocksDB:
		return newRocksDBInMem(attrs, cacheSize)
	}
	panic(fmt.Sprintf("unknown engine type: %d", engine))
}

// NewDefaultInMem allocates and returns a new, opened in-memory engine with
// the default configuration. The caller must call the engine's Close method
// when the engine is no longer needed.
func NewDefaultInMem() Engine {
	return NewInMem(TestStorageEngine, roachpb.Attributes{}, 1<<20 /* 1 MB */)
}

// stickyInMemEngine extends a normal engine, but does not allow them to be
// closed using the normal Close() method, instead keeping the engines in
// memory until CloseAllStickyInMemEngines is called, hence being "sticky".
// This prevents users of the in memory engine from having to special
// case "sticky" engines on every instance of "Close".
// It is intended for use in demos and/or tests, where we want in-memory
// storage nodes to persist between killed nodes.
type stickyInMemEngine struct {
	// id is the unique identifier for this sticky engine.
	id string
	// closed indicates whether the current engine has been closed.
	closed bool

	// engineType used to initialize the engine.
	engineType enginepb.EngineType
	// attrs used to initialize the engine.
	attrs roachpb.Attributes
	// cacheSize used to initialize the engine.
	cacheSize int64

	// Engine extends the Engine interface.
	Engine
}

// stickyInMemEngine implements Engine.
var _ Engine = &stickyInMemEngine{}

// Close overwrites the default Engine interface to not close the underlying
// engine if called. We mark the state as closed to reflect a correct result
// in Closed().
func (e *stickyInMemEngine) Close() {
	e.closed = true
}

// Closed overwrites the default Engine interface.
func (e *stickyInMemEngine) Closed() bool {
	return e.closed
}

// stickyInMemEnginesCache is the bookkeeper for all active
// sticky engines, keyed by their id.
var stickyInMemEnginesCache map[string]*stickyInMemEngine = map[string]*stickyInMemEngine{}

// GetStickyInMemEngine returns an engine associated with the given id.
// It will create a new in-memory engine if one does not already exist.
// At most one engine with a given id can be active in
// "GetOrCreateStickyInMemEngine" at any given time.
// One must Close() on the sticky engine before another can be fetched.
func GetOrCreateStickyInMemEngine(
	ctx context.Context,
	id string,
	engineType enginepb.EngineType,
	attrs roachpb.Attributes,
	cacheSize int64,
) (Engine, error) {
	if engine, ok := stickyInMemEnginesCache[id]; ok {
		if !engine.closed {
			return nil, errors.Errorf("sticky engine %s has not been closed", id)
		}

		log.Infof(ctx, "re-using sticky in-mem engine %s", id)
		engine.closed = false
		return engine, nil
	}

	log.Infof(ctx, "creating new sticky in-mem engine %s", id)
	engine := &stickyInMemEngine{
		id:     id,
		closed: false,

		engineType: engineType,
		attrs:      attrs,
		cacheSize:  cacheSize,

		Engine: NewInMem(engineType, attrs, cacheSize),
	}
	stickyInMemEnginesCache[id] = engine
	return engine, nil
}

// CloseStickyInMemEngine closes the underlying engine and
// removes the sticky engine keyed by the given id.
// It will error if it does not exist.
func CloseStickyInMemEngine(id string) error {
	if engine, ok := stickyInMemEnginesCache[id]; ok {
		engine.closed = true
		engine.Engine.Close()
		delete(stickyInMemEnginesCache, id)
		return nil
	}
	return errors.Errorf("sticky in-mem engine %s does not exist", id)
}

// CloseAllStickyInMemEngines closes and removes all sticky in memory engines.
func CloseAllStickyInMemEngines() {
	for _, engine := range stickyInMemEnginesCache {
		engine.closed = true
		engine.Engine.Close()
	}
	stickyInMemEnginesCache = map[string]*stickyInMemEngine{}
}
