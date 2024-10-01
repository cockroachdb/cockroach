// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package contentionutils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

type pair struct {
	k uuid.UUID
	v int
}

// testAsyncBuffer is a simple asynchronous lock-free buffer implemented using
// ConcurrentBufferGuard. It serves two purposes:
// 1. provide a simple testing interface to test ConcurrentBufferGuard.
// 2. provide a simple example on how ConcurrentBufferGuard can be used.
type testAsyncBuffer struct {
	guard *ConcurrentBufferGuard

	writerBuffer []pair

	// zeroBuffer is used to quickly reset writerBuffer using Golang's builtin
	// copy.
	zeroBuffer []pair

	// validation is an anonymous struct that synchronizes the writes to the
	// testAsyncBuffer for testing purposes. Alternatively, this can be
	// implemented using Golang's channel.
	validation struct {
		syncutil.RWMutex
		readMap map[uuid.UUID]int
	}
}

// newTestBuffer creates a new testAsyncBuffer. The sizeLimit params specify
// the size of the writerBuffer before it gets flushed.
func newTestBuffer(sizeLimit int64) *testAsyncBuffer {
	t := &testAsyncBuffer{
		writerBuffer: make([]pair, sizeLimit),
		zeroBuffer:   make([]pair, sizeLimit),
	}

	t.validation.readMap = make(map[uuid.UUID]int)

	t.guard = NewConcurrentBufferGuard(
		func() int64 {
			return sizeLimit
		}, /* limiter */
		func(currentWriterIdx int64) {
			t.validation.Lock()
			for idx := int64(0); idx < currentWriterIdx; idx++ {
				p := t.writerBuffer[idx]
				t.validation.readMap[p.k] = p.v
			}
			t.validation.Unlock()

			// Resets t.writerBuffer.
			copy(t.writerBuffer, t.zeroBuffer)
		}, /* onBufferFullSync */
	)

	return t
}

func (ta *testAsyncBuffer) write(v pair) {
	ta.guard.AtomicWrite(func(writerIdx int64) {
		ta.writerBuffer[writerIdx] = v
	})
}

func (ta *testAsyncBuffer) sync() {
	ta.guard.ForceSync()
}

func (ta *testAsyncBuffer) assert(t *testing.T, expectedMap map[uuid.UUID]int) {
	t.Helper()

	ta.validation.RLock()
	defer ta.validation.RUnlock()

	for k, v := range expectedMap {
		actual, ok := ta.validation.readMap[k]
		require.True(t, ok,
			"expected %s to exist, but it was not found", k.String())
		require.Equal(t, v, actual, "expected to found pair %s:%d, but "+
			"found %s:%d", k.String(), v, k.String(), actual)
	}
}

func TestConcurrentWriterGuard(t *testing.T) {
	numOfConcurrentWriters := []int{1, 2, 4, 16, 32}
	bufferSizeLimit := []int64{1, 2, 5, 10, 20, 48}
	for _, concurrentWriters := range numOfConcurrentWriters {
		t.Run(fmt.Sprintf("concurrentWriter=%d", concurrentWriters), func(t *testing.T) {
			for _, sizeLimit := range bufferSizeLimit {
				t.Run(fmt.Sprintf("bufferSizeLimit=%d", sizeLimit), func(t *testing.T) {
					runConcurrentWriterGuard(t, concurrentWriters, sizeLimit)
				})
			}
		})
	}
}

func TestConcurrentBufferGuard_ForceSyncExec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("executes function prior to onBufferFullSync", func(t *testing.T) {
		// Construct a guard whose onBufferFullSync fn simply checks that a
		// condition was set prior to execution. We can later make assertions
		// on the result.
		conditionSet := false
		fnCalled := false
		guard := NewConcurrentBufferGuard(
			func() int64 {
				return 1024 // 1K
			}, /* limiter */
			func(currentWriterIdx int64) {
				fnCalled = conditionSet
			}, /* onBufferFullSync */
		)
		// Execute and verify our func was called prior to the
		// onBufferFullSync call.
		guard.ForceSyncExec(func() {
			conditionSet = true
		})
		require.True(t, fnCalled)
	})
}

func runConcurrentWriterGuard(t *testing.T, concurrentWriters int, sizeLimit int64) {
	start := make(chan struct{})
	buf := newTestBuffer(sizeLimit)

	expectedMaps := make(chan map[uuid.UUID]int, concurrentWriters)

	var wg sync.WaitGroup

	for writerCnt := 0; writerCnt < concurrentWriters; writerCnt++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			input, expected := randomGeneratedInput()
			expectedMaps <- expected

			<-start

			for _, val := range input {
				buf.write(val)
			}
		}()
	}
	close(start)

	wg.Wait()

	buf.sync()
	for writerIdx := 0; writerIdx < concurrentWriters; writerIdx++ {
		expected := <-expectedMaps
		buf.assert(t, expected)
	}
}

func randomGeneratedInput() (input []pair, expected map[uuid.UUID]int) {
	const inputSize = 2000
	input = make([]pair, 0, inputSize)
	expected = make(map[uuid.UUID]int)

	p := pair{}
	for i := 0; i < inputSize; i++ {
		p.k = uuid.MakeV4()
		p.v = rand.Int()
		input = append(input, p)
		expected[p.k] = p.v
	}

	return input, expected
}
