// Copyright 2018 The Cockroach Authors.
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

package engine

import (
	"bytes"
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// MigrationIterator wraps a SimpleIterator and provides a backward compatible
// view of its key/value pairs. It does so by translating new keys that may not
// exist on all replicas in a mixed-version cluster into their corresponding
// legacy representations. This "virtual migration" of the keyspace makes it
// possible for state to diverge between replicas while each still exposes a
// consistent persistent state to operations that require one, such as
// consistency checks and stats computations.
//
// Of course, extreme care must still be taken to ensure that the
// inconsistencies masked by this iterator are handled safely. It is still
// favorable to perform a consistent migration when possible because this can
// reduce long-term complexity and avoids the overhead of synthesizing the
// legacy key/values.
//
// Virtual migrations (add to this list):
// - hide replicated RaftTombstone key (#21120).
// - translate RangeAppliedState key into legacy RaftAppliedIndex,
//   LeaseAppliedIndex, and RangeStats keys (#22317).
type MigrationIterator struct {
	it SimpleIterator

	// Fields storing synthesized state.
	synthErr error
	synthVal []byte
	tmpMeta  enginepb.MVCCMetadata
	tmpValue roachpb.Value

	// Fields supporting the range applied state migration.
	lastRangeAppliedState   enginepb.RangeAppliedState
	lastRangeAppliedStateID roachpb.RangeID
}

// NewMigrationIterator creates a new MigrationIterator, wrapping the
// provided iterator.
func NewMigrationIterator(it SimpleIterator) SimpleIterator {
	return &MigrationIterator{it: it}
}

// Close implements the SimpleIterator interface.
func (mi *MigrationIterator) Close() { mi.it.Close() }

// Seek implements the SimpleIterator interface.
func (mi *MigrationIterator) Seek(key MVCCKey) { mi.it.Seek(key) }

// Valid implements the SimpleIterator interface.
func (mi *MigrationIterator) Valid() (bool, error) {
	if mi.synthErr != nil {
		return false, mi.synthErr
	}
	return mi.it.Valid()
}

// Next implements the SimpleIterator interface.
func (mi *MigrationIterator) Next() {
	mi.resetSynthesizedState()
	mi.it.Next()
	if mi.maybeSynthesizeKey() {
		mi.Next()
	}
}

// NextKey implements the SimpleIterator interface.
func (mi *MigrationIterator) NextKey() {
	mi.resetSynthesizedState()
	mi.it.NextKey()
	if mi.maybeSynthesizeKey() {
		mi.Next()
	}
}

// UnsafeKey implements the SimpleIterator interface.
func (mi *MigrationIterator) UnsafeKey() MVCCKey {
	return mi.it.UnsafeKey()
}

// UnsafeValue implements the SimpleIterator interface.
func (mi *MigrationIterator) UnsafeValue() []byte {
	if mi.synthVal != nil {
		return mi.synthVal
	}
	return mi.it.UnsafeValue()
}

// resetSynthesizedState resets any state retained from a previously synthesized
// key/value pair.
func (mi *MigrationIterator) resetSynthesizedState() {
	mi.synthErr = nil
	mi.synthVal = nil
}

// maybeSynthesizeKey examines the current key and decides whether it needs
// to be translated to a legacy representations. If so, it will perform the
// synthesis. The method returns whether or not the current key should be
// skipped.
func (mi *MigrationIterator) maybeSynthesizeKey() (skip bool) {
	ok, err := mi.it.Valid()
	if !ok || err != nil {
		return false
	}

	unsafeKey := mi.it.UnsafeKey()
	unsafeValue := mi.it.UnsafeValue()

	// Range-local Range ID key.
	if bytes.HasPrefix(unsafeKey.Key, keys.LocalRangeIDPrefix) {
		rangeID, infix, suffix, _ /* detail */, err := keys.DecodeRangeIDKey(unsafeKey.Key)
		if err != nil {
			mi.synthErr = errors.Wrap(err, "unable to decode rangeID key")
			return false
		}

		// Replicated range-local Range ID key.
		if infix.Equal(keys.LocalRangeIDReplicatedInfix) {
			// Replicated range-local raft tombstone key. Ignore.
			if suffix.Equal(keys.LocalRaftTombstoneSuffix) {
				return true // skip
			}

			// Replicated range-local range applied state key. Decode, cache, and ignore.
			if suffix.Equal(keys.LocalRangeAppliedStateSuffix) {
				mi.lastRangeAppliedStateID = rangeID

				defer mi.tmpMeta.Reset()
				if err := protoutil.Unmarshal(unsafeValue, &mi.tmpMeta); err != nil {
					mi.synthErr = errors.Wrap(err, "unable to decode MVCCMetadata")
					return false
				}
				if err := MakeValue(mi.tmpMeta).GetProto(&mi.lastRangeAppliedState); err != nil {
					mi.synthErr = errors.Wrap(err, "unable to decode RangeAppliedState")
					return false
				}
				return true // skip
			}

			// Replicated range-local keys replaced by range applied state key.
			// Synthesize using cached range applied state key.
			rai := suffix.Equal(keys.LocalRaftAppliedIndexLegacySuffix)
			lai := suffix.Equal(keys.LocalLeaseAppliedIndexLegacySuffix)
			rms := suffix.Equal(keys.LocalRangeStatsLegacySuffix)
			if rai || lai || rms {
				if rangeID != mi.lastRangeAppliedStateID {
					// This is possible if a range applied state key hasn't been
					// written yet, in which case this key should be up-to-date.
					log.Infof(context.TODO(), "found legacy key %s without corresponding range "+
						"applied state key; cannot synthesize", unsafeKey)
					return false
				}

				defer mi.tmpValue.Reset()
				switch {
				case rai:
					mi.tmpValue.SetInt(int64(mi.lastRangeAppliedState.RaftAppliedIndex))
				case lai:
					mi.tmpValue.SetInt(int64(mi.lastRangeAppliedState.LeaseAppliedIndex))
				case rms:
					rasMS := mi.lastRangeAppliedState.RangeStats.ToStats()
					if err := mi.tmpValue.SetProto(&rasMS); err != nil {
						mi.synthErr = errors.Wrap(err, "unable to encode MVCCStats")
						return false
					}
					// Make sure that this checksum is always set.
					mi.tmpValue.InitChecksum(unsafeKey.Key)
				}

				defer mi.tmpMeta.Reset()
				mi.tmpMeta.RawBytes = mi.tmpValue.RawBytes
				if mi.synthVal, err = protoutil.Marshal(&mi.tmpMeta); err != nil {
					mi.synthErr = errors.Wrap(err, "unable to encode MVCCMetadata")
					return false
				}
			}
		}
	}
	return false
}
