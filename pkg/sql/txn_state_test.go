// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// failingRewinder is an implementation of rewindInterface that stores an error
// if getRewindCapability() is called. setTxnStartPos is allowed, and the start
// position is saved.
// close() needs to be called to assert that getRewindCapability has not been
// called.
type failingRewinder struct {
	err         error
	txnStartPos cursorPosition
}

// close() returns an error if getRewindTxnCapability() has been called.
func (f *failingRewinder) close() error {
	return f.err
}

// getRewindCapability is part of rewindInterface.
func (f *failingRewinder) getRewindTxnCapability() (rewindCapability, bool) {
	f.err = errors.Errorf("getRewindTxnCapability called")
	return rewindCapability{}, false
}

// setTxnStartPos is part of rewindInterface.
func (f *failingRewinder) setTxnStartPos(_ context.Context, pos cursorPosition) {
	f.txnStartPos = pos
}

const noFlush bool = false
const flushSet bool = true

var noRewind = cursorPosition{queryStrPos: -1, stmtIdx: -1}

// checkAdv returns an error if adv does not match all the expected fields.
//
// Pass noFlush/flushSet for expFlush. Pass noRewind for expRewPos if a rewind
// is not expected.
func checkAdv(adv advanceInfo, expCode advanceCode, expFlush bool, expRewPos cursorPosition) error {
	if adv.code != expCode {
		return errors.Errorf("expected code: %s, but got: %+v", expCode, adv)
	}
	if adv.flush != expFlush {
		return errors.Errorf("expected flush: %t, but got: %+v", expFlush, adv)
	}
	if expRewPos == noRewind {
		if adv.rewCap != (rewindCapability{}) {
			return errors.Errorf("expected not rewind, but got: %+v", adv)
		}
	} else {
		if adv.rewCap.rewindPos != expRewPos {
			return errors.Errorf("expected rewind to %s, but got: %+v", expRewPos, adv)
		}
	}
	return nil
}

// expKVTxn is used with checkTxn to check that fields on a client.Txn
// correspond to expectations. Any field left nil will not be checked.
type expKVTxn struct {
	debugName    *string
	isolation    *enginepb.IsolationType
	userPriority *roachpb.UserPriority
	// For the timestamps we just check the physical part. The logical part is
	// incremented every time the clock is read and so it's unpredictable.
	tsNanos     *int64
	origTSNanos *int64
	maxTSNanos  *int64
}

func checkTxn(txn *client.Txn, exp expKVTxn) error {
	if txn == nil {
		return errors.Errorf("uninitialized txn")
	}
	if exp.debugName != nil && !strings.HasPrefix(txn.DebugName(), *exp.debugName) {
		return errors.Errorf("expected DebugName: %s, but got: %s",
			*exp.debugName, txn.DebugName())
	}
	if exp.isolation != nil && *exp.isolation != txn.Isolation() {
		return errors.Errorf("expected isolation: %s, but got: %s",
			*exp.isolation, txn.Isolation())
	}
	if exp.userPriority != nil && *exp.userPriority != txn.UserPriority() {
		return errors.Errorf("expected UserPriority: %s, but got: %s",
			*exp.userPriority, txn.UserPriority())
	}
	if exp.tsNanos != nil && *exp.tsNanos != txn.Proto().Timestamp.WallTime {
		return errors.Errorf("expected Timestamp: %d, but got: %s",
			*exp.tsNanos, txn.Proto().Timestamp)
	}
	if exp.origTSNanos != nil && *exp.origTSNanos != txn.OrigTimestamp().WallTime {
		return errors.Errorf("expected OrigTimestamp: %d, but got: %s",
			*exp.origTSNanos, txn.OrigTimestamp())
	}
	if exp.maxTSNanos != nil && *exp.maxTSNanos != txn.Proto().MaxTimestamp.WallTime {
		return errors.Errorf("expected MaxTimestamp: %d, but got: %s",
			*exp.maxTSNanos, txn.Proto().MaxTimestamp)
	}
	return nil
}

func TestTransitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	firstStmtPos := cursorPosition{
		queryStrPos: 10,
		stmtIdx:     20,
	}
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	mockDB := client.NewDB(nil /* sender */, clock)
	mon := mon.MakeMonitor(
		"test mon",
		mon.MemoryResource,
		nil,  /* curCount */
		nil,  /* maxHist */
		1000, /* increment */
		1000, /* noteworthy */
	)
	tranCtx := transitionCtx{
		db:                    mockDB,
		nodeID:                roachpb.NodeID(5),
		clock:                 clock,
		connMon:               &mon,
		tracer:                tracing.NewTracer(),
		DefaultIsolationLevel: enginepb.SERIALIZABLE,
	}

	type expAdvance struct {
		expCode advanceCode
		// Use noFlush/flushSet.
		expFlush bool
		// Use noRewind if a rewind is not expected.
		expRewPos cursorPosition
	}

	type initTxnState func() *txnState2

	implicitTxnName := sqlImplicitTxnName
	explicitTxnName := sqlTxnName
	now := clock.Now()
	iso := enginepb.SERIALIZABLE
	pri := roachpb.NormalUserPriority
	maxTS := now.Add(clock.MaxOffset().Nanoseconds(), 0 /* logical */)
	type test struct {
		name string

		// A function used to init the txnState in the desired state before the
		// transition.
		init initTxnState

		// The transition to perform.
		tran transition

		// The expected state after the transition.
		expState TxnStateEnum2

		// The expected advance instructions resulting from the transition.
		expAdv expAdvance

		// If nil, the kv txn is expected to be nil.
		expTxn *expKVTxn
	}
	tests := []test{
		{
			// Start an implicit txn from NoTxn.
			name: "NoTxn->Starting (implicit txn)",
			init: func() *txnState2 {
				ts := txnState2{
					state: StateNoTxn,
				}
				return &ts
			},
			tran:     makeBeginTxnTransition(implicitTxn, firstStmtPos),
			expState: StateOpen,
			expAdv: expAdvance{
				// We expect to stayInPlace; upon starting a txn the statement is
				// executed again, this time in state Open.
				expCode:   stayInPlace,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: &expKVTxn{
				debugName:    &implicitTxnName,
				isolation:    &iso,
				userPriority: &pri,
				tsNanos:      &now.WallTime,
				origTSNanos:  &now.WallTime,
				maxTSNanos:   &maxTS.WallTime,
			},
		},
		{
			// Start an explicit txn from NoTxn.
			name: "NoTxn->Starting (explicit txn)",
			init: func() *txnState2 {
				ts := txnState2{
					state: StateNoTxn,
				}
				return &ts
			},
			tran:     makeBeginTxnTransition(implicitTxn, firstStmtPos),
			expState: StateOpen,
			expAdv: expAdvance{
				expCode:   stayInPlace,
				expFlush:  flushSet,
				expRewPos: noRewind,
			},
			expTxn: &expKVTxn{
				debugName:    &explicitTxnName,
				isolation:    &iso,
				userPriority: &pri,
				tsNanos:      &now.WallTime,
				origTSNanos:  &now.WallTime,
				maxTSNanos:   &maxTS.WallTime,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rew := failingRewinder{}
			defer func() {
				if err := rew.close(); err != nil {
					t.Fatal(err)
				}
			}()

			// Get the initial state.
			ts := tc.init()

			// Perform the test's transition.
			adv, err := ts.performStateTransition(ctx, tc.tran, &rew, firstStmtPos, tranCtx)
			if err != nil {
				t.Fatal(err)
			}

			// Check that we moved to the right high-level state.
			if ts.state != tc.expState {
				t.Fatalf("expected state tc.expState, got: %s", ts.state)
			}

			// Check the resulting advanceInfo.
			if err := checkAdv(
				adv, tc.expAdv.expCode, tc.expAdv.expFlush, tc.expAdv.expRewPos,
			); err != nil {
				t.Fatal(err)
			}

			// Check that the KV txn has been properly initialized.
			if tc.expTxn == nil {
				if ts.mu.txn != nil {
					t.Fatalf("expected no txn, got: %v", ts.mu.txn)
				}
			} else {
				if err := checkTxn(ts.mu.txn, *tc.expTxn); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
