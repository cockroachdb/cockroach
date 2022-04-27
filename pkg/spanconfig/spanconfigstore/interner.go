// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type internerID uint64

// interner interns span config protos. It's a ref-counted data structure that
// maintains a single copy of every unique config that that's been added to
// it. Configs that are maintained are identified/retrivable using an
// internerID. Configs can also be removed, and if there are no more references
// to it, we no longer hold onto it in memory.
type interner struct {
	internIDAlloc internerID

	configToID   map[string]internerID
	idToConfig   map[internerID]roachpb.SpanConfig
	idToRefCount map[internerID]uint64
}

func newInterner() *interner {
	return &interner{
		configToID:   make(map[string]internerID),
		idToConfig:   make(map[internerID]roachpb.SpanConfig),
		idToRefCount: make(map[internerID]uint64),
	}
}

func (i *interner) add(ctx context.Context, conf roachpb.SpanConfig) internerID {
	marshalled, err := conf.Marshal()
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	if id, found := i.configToID[string(marshalled)]; found {
		i.idToRefCount[id] += 1
		return id
	}

	i.internIDAlloc++

	id := i.internIDAlloc
	i.configToID[string(marshalled)] = id
	i.idToConfig[i.internIDAlloc] = conf
	i.idToRefCount[id] = 1
	return id
}

func (i *interner) get(id internerID) (roachpb.SpanConfig, bool) {
	conf, found := i.idToConfig[id]
	return conf, found
}

func (i *interner) remove(ctx context.Context, id internerID) {
	conf, found := i.idToConfig[id]
	if !found {
		return // nothing to do
	}

	i.idToRefCount[id] -= 1
	if i.idToRefCount[id] != 0 {
		return // nothing to do
	}

	marshalled, err := conf.Marshal()
	if err != nil {
		log.Infof(ctx, "%v", err)
	}

	delete(i.idToConfig, id)
	delete(i.idToRefCount, id)
	delete(i.configToID, string(marshalled))
}
