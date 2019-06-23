// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	disableMergesInterval = 10 * time.Second
)

// DisableMerges starts a goroutine which periodically gossips keys that
// disable merging for the specified table IDs. The goroutine until the
// associated context is done (usually via cancellation).
func (g *Gossip) DisableMerges(ctx context.Context, tableIDs []uint32) {
	if len(tableIDs) == 0 {
		// Nothing to do.
		return
	}

	disable := func() {
		for _, id := range tableIDs {
			key := MakeTableDisableMergesKey(id)
			err := g.AddInfo(key, nil /* value */, disableMergesInterval*2 /* ttl */)
			if err != nil {
				log.Infof(ctx, "failed to gossip: %s: %v", key, err)
			}
		}
	}

	// Disable merging synchronously before we start the periodic loop below.
	disable()

	s := g.Stopper()
	// We don't care if this task can't be started as that only occurs if the
	// stopper is stopping.
	_ = s.RunAsyncTask(ctx, "disable-merges", func(ctx context.Context) {
		ticker := time.NewTicker(disableMergesInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				disable()
			case <-ctx.Done():
				return
			case <-s.ShouldQuiesce():
				return
			}
		}
	})
}
