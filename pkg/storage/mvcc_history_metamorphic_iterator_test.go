// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package storage_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// metamorphicIterator extracts more coverage out of TestMVCCHistories by, after each
// repositioning operation, moving the iterator around a bit but leaving it in what
// ought to be the same state.
type metamorphicIterator struct {
	buf  strings.Builder
	seed int64
	t    *testing.T
	r    *rand.Rand
	it   storage.SimpleMVCCIterator
	// isForward is true if the wrapped iterator is in forward mode at the
	// beginning of moveAround. We then need to leave the iterator in forward mode
	// because the caller might subsequently invoke NextKey which is illegal on an
	// iterator in reverse direction.
	isForward                   bool
	rangeKeyChanged             bool // preserves original RangeKeyChanged since moving around underneath will mess with it
	rangeKeyChangedIgnoringTime bool // ditto for RangeKeyChangedIgnoringTime if `it` is an MVCCIncrementalIterator
	actions                     int  // counter for logging
}

// newMetamorphicIterator returns a SimpleIterator that is backed by either
// - a metamorphicMVCCIterator, if `it` is an MVCCIterator
// - a metamorphicMVCCIncrementalIterator, if `it` is an MVCCIncrementalIterator
// a metamorphicIterator otherwise.
func newMetamorphicIterator(
	t *testing.T, seed int64, it storage.SimpleMVCCIterator,
) storage.SimpleMVCCIterator {
	iter := &metamorphicIterator{t: t, seed: seed, r: rand.New(rand.NewSource(seed)), it: it}
	if _, isMVCC := it.(storage.MVCCIterator); isMVCC {
		return &metamorphicMVCCIterator{metamorphicIterator: iter}
	}
	if _, isIncremental := it.(*storage.MVCCIncrementalIterator); isIncremental {
		return &metamorphicMVCCIncrementalIterator{metamorphicIterator: iter}
	}
	return iter
}

type action struct {
	name string
	do   func()
}

func (a action) String() string {
	return a.name
}

// moveAround adds extra movement on the underlying iterator after each
// positioning operation.
//
// See newMetamorphicIterator.
func (m *metamorphicIterator) moveAround() {
	if m.seed == 0 {
		return
	}
	m.actions++
	defer func() {
		m.isForward = false
	}()

	var printfln func(string, ...interface{})
	{
		m.buf.Reset()
		defer func() {
			if !m.t.Failed() {
				return
			}
			m.t.Log(m.buf.String())
		}()
		printfln = func(format string, args ...interface{}) {
			_, _ = fmt.Fprintf(&m.buf, format+"\n", args...)
		}
	}

	if valid, err := m.it.Valid(); err != nil || !valid {
		printfln("iter not valid: (%t, %v)", valid, err)
		return
	}

	cur := m.it.UnsafeKey().Clone()
	mvccIt, _ := m.it.(storage.MVCCIterator)
	iit, _ := m.it.(*storage.MVCCIncrementalIterator)
	var resetActions []action

	actions := []action{
		{
			"SeekGE(cur)",
			func() { m.it.SeekGE(cur) },
		},
		{
			"Next",
			func() {
				m.it.Next()
				stillValid, _ := m.it.Valid()
				if stillValid {
					resetActions = append(resetActions, action{
						"ResetViaPrev",
						mvccIt.Prev,
					})
				}
			},
		},
		{
			"SeekGE(Max)",
			func() { m.it.SeekGE(storage.MVCCKeyMax) },
		},
	}

	if iit != nil {
		actions = append(actions, action{
			"NextIgnoringTime",
			iit.NextIgnoringTime,
		}, action{
			"NextKeyIgnoringTime",
			iit.NextKeyIgnoringTime,
		})
	}

	if mvccIt != nil {
		actions = append(actions, action{
			"SeekLT(cur)",
			func() { mvccIt.SeekLT(cur) },
		}, action{
			"SeekLT(Max)",
			func() { mvccIt.SeekLT(storage.MVCCKeyMax) },
		})
		// Can only leave iterator in reverse mode if it's in reverse
		// initially, otherwise caller wouldn't be allowed to invoke NextKey
		// due to MVCCIterator contract.
		if !m.isForward {
			actions = append(actions, action{
				"Prev",
				func() {
					mvccIt.Prev()
					resetActions = append(resetActions, action{
						"ResetViaNext",
						mvccIt.Next,
					})
				},
			})
		}
	}

	hasPoint, _ := m.it.HasPointAndRange()
	rangeKeys := rangeKeysIfExist(m.it).Clone()
	var rangeKeysIgnoringTime storage.MVCCRangeKeyStack
	if iit != nil {
		rangeKeysIgnoringTime = iit.RangeKeysIgnoringTime()
	}
	rangeKeyChanged := m.it.RangeKeyChanged()
	m.rangeKeyChanged = rangeKeyChanged
	if iit != nil {
		m.rangeKeyChangedIgnoringTime = iit.RangeKeyChangedIgnoringTime()
	}
	printfln("original position: %s [hasPoint=%t]; rangeKeysChanged=%t rangeKeys=%s rangeKeysIgnoringTime=%s",
		cur, hasPoint, rangeKeyChanged, rangeKeys, rangeKeysIgnoringTime)
	// If we move the iterator and later fix its position via SeekGE, we might leave
	// and then re-enter the current range key even though it didn't change from the
	// perspective of the caller. We need to override the result in this case, i.e.
	// if this is false make sure that RangeKeyChanged() returns false after we've
	// repositioned.
	choice := actions[m.r.Intn(len(actions))]
	printfln("action: %s", choice)

	// NB: if this is an incr iter it may be ignoring time, so we can't expect SeekGE(cur) to
	// be able to retrieve the current key, as SeekGE always respects the time bound.
	if iit == nil || !iit.IgnoringTime() {
		resetActions = append(resetActions, action{
			"SeekGE(cur)",
			func() {
				m.it.SeekGE(cur)
			},
		})
	}

	if hasPoint && (mvccIt == nil || !mvccIt.IsPrefix()) {
		// If we're not a prefix iter, we should be able to start from KeyMin and
		// walk our way back to where we started.
		resetActions = append(resetActions, action{
			"Seek(min) && Iterate",
			func() {
				if bytes.Compare(cur.Key, roachpb.LocalMax) >= 0 {
					// Make sure we don't put a global-only iter into local keyspace.
					printfln("seeking to LocalMax")
					m.it.SeekGE(storage.MakeMVCCMetadataKey(roachpb.LocalMax))
				} else {
					printfln("seeking to KeyMin")
					m.it.SeekGE(storage.NilKey)
				}
				for {
					valid, err := m.it.Valid()
					require.Nil(m.t, err)
					require.True(m.t, valid, "unable to recover original position following SeekGE")
					if m.it.UnsafeKey().Equal(cur) {
						break // made it
					}
					printfln("step: %s %s [changed=%t]", m.it.UnsafeKey(), rangeKeysIfExist(m.it), m.it.RangeKeyChanged())
					if iit != nil {
						// If we're an incremental iterator with time bounds, and `cur` is not within bounds,
						// would miss it if we used Next. So call NextIgnoringTime unconditionally.
						iit.NextIgnoringTime()
					} else {
						m.it.Next()
					}
				}
			},
		})
	}
	// NB: can't use reverse iteration to find the point if the iterator is
	// currently forward.
	if !m.isForward && hasPoint && !mvccIt.IsPrefix() {
		resetActions = append(resetActions, action{
			"SeekLT(max) && RevIterate",
			func() {
				mvccIt.SeekLT(storage.MVCCKeyMax) // NB: incompatible with IsPrefix, so we excluded that above
				for {
					valid, err := m.it.Valid()
					require.Nil(m.t, err)
					require.True(m.t, valid, "unable to recover original position following SeekLT")
					printfln("rev-step: %s %s [changed=%t]", m.it.UnsafeKey(), rangeKeysIfExist(m.it), m.it.RangeKeyChanged())
					if m.it.UnsafeKey().Equal(cur) {
						printfln("done")
						break // made it
					}
					mvccIt.Prev()
				}
			},
		})
	}

	resetAction := resetActions[m.r.Intn(len(resetActions))]
	printfln("resetting via %s", resetAction.name)
	resetAction.do()
	{
		hasPoint2, _ := m.it.HasPointAndRange() // circumvent hated shadowing lint
		var rangeKeysIgnoringTime2 storage.MVCCRangeKeyStack
		if iit != nil {
			rangeKeysIgnoringTime2 = iit.RangeKeysIgnoringTime()
		}
		printfln("recovered position: %s hasPoint=%t, rangeKeys=%s, rangeKeysIgnoringTime=%s",
			m.it.UnsafeKey(), hasPoint2, rangeKeysIfExist(m.it), rangeKeysIgnoringTime2)
	}
	// Back where we started and hopefully in an indistinguishable state.
	// When the stack is empty, sometimes it's a nil slice and sometimes zero
	// slice. A similar problem exists with MVCCRangeKeyVersion.Value. Sidestep
	// them by comparing strings.
	require.Equal(m.t, fmt.Sprint(rangeKeys), fmt.Sprint(rangeKeysIfExist(m.it)))
	if iit != nil {
		require.Equal(m.t, fmt.Sprint(rangeKeysIgnoringTime), fmt.Sprint(iit.RangeKeysIgnoringTime()))
	}
}

func (m *metamorphicIterator) Close() {
	if m.actions > 0 {
		m.t.Logf("metamorphicIterator: carried out %d actions", m.actions)
		if m.t.Failed() {
			m.t.Logf("metamorphicIterator log:\n%s", m.buf.String())
		}
	}
	m.it.Close()
}

func (m *metamorphicIterator) SeekGE(key storage.MVCCKey) {
	m.isForward = true
	m.it.SeekGE(key)
	m.moveAround()
}

func (m *metamorphicIterator) Valid() (bool, error) {
	return m.it.Valid()
}

func (m *metamorphicIterator) Next() {
	m.it.Next()
	m.isForward = true
	m.moveAround()
}

func (m *metamorphicIterator) NextKey() {
	m.it.NextKey()
	m.isForward = true
	m.moveAround()
}

func (m *metamorphicIterator) UnsafeKey() storage.MVCCKey {
	return m.it.UnsafeKey()
}

func (m *metamorphicIterator) UnsafeValue() ([]byte, error) {
	return m.it.UnsafeValue()
}

func (m *metamorphicIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return m.it.MVCCValueLenAndIsTombstone()
}

func (m *metamorphicIterator) ValueLen() int {
	return m.it.ValueLen()
}

func (m *metamorphicIterator) HasPointAndRange() (bool, bool) {
	return m.it.HasPointAndRange()
}

func (m *metamorphicIterator) RangeBounds() roachpb.Span {
	return m.it.RangeBounds()
}

func (m *metamorphicIterator) RangeKeys() storage.MVCCRangeKeyStack {
	return m.it.RangeKeys()
}

func (m *metamorphicIterator) RangeKeyChanged() bool {
	if m.seed != 0 {
		return m.rangeKeyChanged
	}
	return m.it.RangeKeyChanged()
}

type metamorphicMVCCIterator struct {
	// INVARIANT: metamorphicIterator.it implements MVCCIterator.
	*metamorphicIterator
}

var _ storage.MVCCIterator = (*metamorphicMVCCIterator)(nil)

func (m *metamorphicMVCCIterator) SeekLT(key storage.MVCCKey) {
	m.it.(storage.MVCCIterator).SeekLT(key)
	m.moveAround()
}

func (m *metamorphicMVCCIterator) Prev() {
	m.it.(storage.MVCCIterator).Prev()
	m.moveAround()
}

func (m *metamorphicMVCCIterator) UnsafeLazyValue() pebble.LazyValue {
	return m.it.(storage.MVCCIterator).UnsafeLazyValue()
}

func (m *metamorphicMVCCIterator) UnsafeRawKey() []byte {
	return m.it.(storage.MVCCIterator).UnsafeRawKey()
}

func (m *metamorphicMVCCIterator) UnsafeRawMVCCKey() []byte {
	return m.it.(storage.MVCCIterator).UnsafeRawMVCCKey()
}

func (m *metamorphicMVCCIterator) Value() ([]byte, error) {
	return m.it.(storage.MVCCIterator).Value()
}

func (m *metamorphicMVCCIterator) ValueProto(msg protoutil.Message) error {
	return m.it.(storage.MVCCIterator).ValueProto(msg)
}

func (m *metamorphicMVCCIterator) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (storage.MVCCKey, error) {
	return m.it.(storage.MVCCIterator).FindSplitKey(start, end, minSplitKey, targetSize)
}

func (m *metamorphicMVCCIterator) Stats() storage.IteratorStats {
	// TODO(tbg): these will be wrong since we do extra movement.
	return m.it.(storage.MVCCIterator).Stats()
}

func (m *metamorphicMVCCIterator) IsPrefix() bool {
	return m.it.(storage.MVCCIterator).IsPrefix()
}

type metamorphicMVCCIncrementalIterator struct {
	*metamorphicIterator
}

var _ mvccIncrementalIteratorI = (*metamorphicMVCCIncrementalIterator)(nil)

func (m *metamorphicMVCCIncrementalIterator) RangeKeysIgnoringTime() storage.MVCCRangeKeyStack {
	return m.it.(*storage.MVCCIncrementalIterator).RangeKeysIgnoringTime()
}

func (m *metamorphicMVCCIncrementalIterator) RangeKeyChangedIgnoringTime() bool {
	if m.seed != 0 {
		return m.rangeKeyChangedIgnoringTime
	}
	return m.it.(*storage.MVCCIncrementalIterator).RangeKeyChangedIgnoringTime()
}

func (m *metamorphicMVCCIncrementalIterator) NextIgnoringTime() {
	m.it.(*storage.MVCCIncrementalIterator).NextIgnoringTime()
	m.isForward = true
	m.moveAround()
}

func (m *metamorphicMVCCIncrementalIterator) NextKeyIgnoringTime() {
	m.it.(*storage.MVCCIncrementalIterator).NextKeyIgnoringTime()
	m.isForward = true
	m.moveAround()
}

func (m *metamorphicMVCCIncrementalIterator) TryGetIntentError() error {
	return m.it.(*storage.MVCCIncrementalIterator).TryGetIntentError()
}
