// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessioninit

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	testMon := mon.BoundAccount{}
	c := NewCache(testMon, stop.NewStopper())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		c.loadValueOutsideOfCache(ctx, "test", func(loadCtx context.Context) (interface{}, error) {
			wg.Wait()
			require.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)

			// loadCtx should not be cancelled if the parent ctx is cancelled.
			require.Equal(t, errors.Is(loadCtx.Err(), context.Canceled), false)
			return nil, nil
		})
	}()

	cancel()
	wg.Done()
}
