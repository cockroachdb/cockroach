// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package destroyguard

import (
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func notDone(t *testing.T, g *Guard) {
	t.Helper()
	select {
	case <-g.Done():
		t.Fatal("unexpectedly done")

	default:
	}
}

func notDead(t *testing.T, g *Guard) {
	t.Helper()
	select {
	case <-g.Dead():
		t.Fatal("unexpectedly dead")
	default:
	}
}

func notDoneOrDead(t *testing.T, g *Guard) {
	t.Helper()
	notDone(t, g)
	notDead(t, g)
}

func TestGuard(t *testing.T) {
	err := errors.New("boom")

	t.Run("instant-teardown", func(t *testing.T) {
		var g Guard
		notDoneOrDead(t, &g)
		g.Teardown(err)
		<-g.Done()
		<-g.Dead()
		g.Teardown(err)
		<-g.Done()
		<-g.Dead()
		assert.Equal(t, err, g.Err())
	})

	t.Run("acquire-teardown-release", func(t *testing.T) {
		var g Guard
		assert.NoError(t, g.Acquire())
		notDoneOrDead(t, &g)
		g.Teardown(err)
		notDead(t, &g)

		// Teardown bit should be set.
		assert.Equal(t, g.ref, (uint32(1)<<31)+1)

		assert.Equal(t, err, g.Err())
		assert.Equal(t, err, g.Acquire())

		g.Release()
		<-g.Done()
		<-g.Dead()

		assert.Equal(t, err, g.Err())
		assert.Equal(t, err, g.Acquire())
	})

	t.Run("concurrent", func(t *testing.T) {
		var g Guard

		var wg sync.WaitGroup

		const n = 20
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if i >= n/2 && (i%3) == 0 {
					g.Teardown(err)
					runtime.Gosched()
				}
				if err := g.Acquire(); err != nil {
					return
				}
				if i%2 == 0 {
					runtime.Gosched()
				}
				notDead(t, &g)
				if i%3 == 0 {
					<-g.Done()
				}
				g.Release()
			}(i)
		}

		wg.Wait()
		<-g.Done()
		<-g.Dead()
		assert.Equal(t, g.ref, uint32(1)<<31)
		assert.Error(t, g.Err())
	})
}
