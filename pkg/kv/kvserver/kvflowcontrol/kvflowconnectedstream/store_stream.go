// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowconnectedstream

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StoreStreamsTokenCounter is one per node.
type StoreStreamsTokenCounter interface {
	// EvalTokenCounterForStrea returns the eval token counter for the given
	// stream.
	EvalTokenCounterForStream(kvflowcontrol.Stream) TokenCounter
	// SendTokenCounterForStream returns the send token counter for the given
	// stream.
	SendTokenCounterForStream(kvflowcontrol.Stream) TokenCounter
}

type storeStreamsTokenCounter struct {
	settings *cluster.Settings
	clock    *hlc.Clock

	mu struct {
		syncutil.Mutex
		sendCounters, evalCounters map[kvflowcontrol.Stream]TokenCounter
	}
}

func (sstc *storeStreamsTokenCounter) EvalTokenCounterForStream(
	stream kvflowcontrol.Stream,
) TokenCounter {
	sstc.mu.Lock()
	defer sstc.mu.Unlock()

	if _, ok := sstc.mu.evalCounters[stream]; !ok {
		sstc.mu.evalCounters[stream] = newTokenCounter(sstc.settings, sstc.clock)
	}
	return sstc.mu.evalCounters[stream]
}

func (sstc *storeStreamsTokenCounter) SendTokenCounterForStream(
	stream kvflowcontrol.Stream,
) TokenCounter {
	sstc.mu.Lock()
	defer sstc.mu.Unlock()

	if _, ok := sstc.mu.sendCounters[stream]; !ok {
		sstc.mu.sendCounters[stream] = newTokenCounter(sstc.settings, sstc.clock)
	}
	return sstc.mu.sendCounters[stream]
}

func NewStoreStreamsTokenCounter(
	settings *cluster.Settings, clock *hlc.Clock,
) StoreStreamsTokenCounter {
	sstc := &storeStreamsTokenCounter{
		settings: settings,
		clock:    clock,
	}

	sstc.mu.sendCounters = make(map[kvflowcontrol.Stream]TokenCounter)
	sstc.mu.evalCounters = make(map[kvflowcontrol.Stream]TokenCounter)
	return sstc
}

// TokenCounter will be implemented by tokenCounter.
type TokenCounter interface {
	// TokensAvailable returns true if tokens are available. If false, it
	// returns a handle to use for waiting using
	// kvflowcontroller.WaitForHandlesAndChannels. This is for waiting
	// pre-evaluation.
	TokensAvailable(admissionpb.WorkClass) (available bool, tokenWaitingHandle TokenWaitingHandle)
	TryDeduct(
		context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens) kvflowcontrol.Tokens
	// Deduct deducts (without blocking) flow tokens for the given priority.
	Deduct(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
	// Return returns flow tokens for the given priority.
	Return(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
	// String returns a string representation of the token counter.
	String() string
}

// TokenWaitingHandle is the interface for waiting for positive tokens.
type TokenWaitingHandle interface {
	// WaitChannel is the channel that will be signaled if tokens are possibly
	// available. If signaled, the caller must call
	// TryDeductAndUnblockNextWaiter.
	WaitChannel() <-chan struct{}
	// TryDeductAndUnblockNextWaiter is called to deduct some tokens. The tokens
	// parameter can be zero, when the waiter is only waiting for positive
	// tokens (such as when waiting before eval). granted <= tokens and the
	// tokens that have been deducted. haveTokens is true iff there are tokens
	// available after this grant. When the tokens parameter is zero, granted
	// will be zero, and haveTokens represents whether there were positive
	// tokens. If the caller is unsatisfied with the return values, it can
	// resume waiting using WaitChannel.
	TryDeductAndUnblockNextWaiter(tokens kvflowcontrol.Tokens) (granted kvflowcontrol.Tokens, haveTokens bool)
}

// StoreStreamSendTokensWatcher implements a watcher interface that will use
// at most one goroutine per SendTokenCounter and WorkClass that has no send
// tokens. Replicas (from different ranges) waiting for those tokens will call
// NotifyWhenAvailable to queue up for those send tokens.
type StoreStreamSendTokensWatcher interface {
	// NotifyWhenAvailable queues up for tokens for a given token counter and
	// work class. When tokens are available, tokensGrantedNotification is
	// called. It is the caller's responsibility to call CancelHandle() when
	// tokens are no longer needed, or when the caller is done.
	NotifyWhenAvailable(
		stc TokenCounter,
		wc admissionpb.WorkClass,
		tokensGrantedNotification TokenAvailableNotification,
	) StoreStreamSendTokenHandleID
	// UpdateHandle updates the given handle to watch the given work class,
	// removing it from watching the existing work class, if the work class is
	// different.
	UpdateHandle(handle StoreStreamSendTokenHandleID, wc admissionpb.WorkClass)
	// CancelHandle cancels the given handle, stopping it from being notified
	// when tokens are available.
	CancelHandle(handle StoreStreamSendTokenHandleID)
	// String returns a string representation of the store stream send tokens
	// watcher.
	String() string
}

const InvalidStoreStreamSendTokenHandleID StoreStreamSendTokenHandleID = 0

// TokenAvailableNotification is an interface that is called when tokens are
// available.
type TokenAvailableNotification interface {
	// Notify is called when tokens are available to be granted.
	Notify()
}

// StoreStreamSendTokenHandleID is a unique identifier for a handle that is
// watching store stream send tokens.
type StoreStreamSendTokenHandleID int64

// StoreStreamSendTokenHandle is a handle that is used to identify and notify
// the caller when store stream send tokens are available.
type StoreStreamSendTokenHandle struct {
	id           StoreStreamSendTokenHandleID
	stc          TokenCounter
	wc           admissionpb.WorkClass
	notification TokenAvailableNotification
}

// NewStoreStreamSendTokensWatcher creates a new StoreStreamSendTokensWatcher.
func NewStoreStreamSendTokensWatcher(stopper *stop.Stopper) *storeStreamSendTokensWatcher {
	ssstw := &storeStreamSendTokensWatcher{stopper: stopper}
	ssstw.mu.watchers = make(map[TokenCounter]*tokenWatcher)
	ssstw.mu.handles = make(map[StoreStreamSendTokenHandleID]*StoreStreamSendTokenHandle)
	ssstw.mu.idSeq = 1
	return ssstw
}

func (s *storeStreamSendTokensWatcher) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf strings.Builder
	handleIDs := make([]StoreStreamSendTokenHandleID, 0, len(s.mu.handles))
	for id := range s.mu.handles {
		handleIDs = append(handleIDs, id)
	}
	sort.Slice(handleIDs, func(i, j int) bool {
		return handleIDs[i] < handleIDs[j]
	})

	buf.WriteString("handles [")
	i := 0
	for _, handle := range s.mu.handles {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "(%v,%v)", handle.id, handle.wc)
		i++
	}

	buf.WriteString("]")
	return buf.String()
}

// storeStreamSendTokensWatcher implements the StoreStreamSendTokensWatcher
// interface.
type storeStreamSendTokensWatcher struct {
	stopper *stop.Stopper

	mu struct {
		syncutil.Mutex

		// idSeq is used to generate unique IDs for each handle.
		idSeq    StoreStreamSendTokenHandleID
		watchers map[TokenCounter]*tokenWatcher
		handles  map[StoreStreamSendTokenHandleID]*StoreStreamSendTokenHandle
	}
}

type tokenWatcher struct {
	tracked map[admissionpb.WorkClass][]StoreStreamSendTokenHandleID
}

func (s *tokenWatcher) removeHandleLocked(handle StoreStreamSendTokenHandle) {
	for i := range s.tracked[handle.wc] {
		if s.tracked[handle.wc][i] == handle.id {
			s.tracked[handle.wc] = append(s.tracked[handle.wc][:i], s.tracked[handle.wc][i+1:]...)
			return
		}
	}
}

// NotifyWhenAvailable queues up for tokens for a given token counter and
// work class. When tokens are available, tokensGrantedNotification is
// called. It is the caller's responsibility to call CancelHandle() when
// tokens are no longer needed, or when the caller is done.
func (s *storeStreamSendTokensWatcher) NotifyWhenAvailable(
	stc TokenCounter, wc admissionpb.WorkClass, tokensGrantedNotification TokenAvailableNotification,
) StoreStreamSendTokenHandleID {
	s.mu.Lock()
	defer s.mu.Unlock()

	handle := StoreStreamSendTokenHandle{
		id:           s.mu.idSeq,
		stc:          stc,
		wc:           wc,
		notification: tokensGrantedNotification,
	}
	s.mu.idSeq++

	s.mu.handles[handle.id] = &handle
	watcher, ok := s.mu.watchers[stc]
	if !ok {
		watcher = &tokenWatcher{
			tracked: make(map[admissionpb.WorkClass][]StoreStreamSendTokenHandleID),
		}
		s.mu.watchers[stc] = watcher
	}
	watcher.tracked[wc] = append(watcher.tracked[wc], handle.id)

	// This is the first token for the work class, token counter pair, launch a
	// new watcher task.
	if len(watcher.tracked[handle.wc]) == 1 {
		s.watchTokens(context.Background(), handle.stc, wc, watcher)
	}

	return handle.id
}

// UpdateHandle updates the given handle to watch the given work class,
// removing it from watching the existing work class, if the work class is
// different.
func (s *storeStreamSendTokensWatcher) UpdateHandle(
	handleID StoreStreamSendTokenHandleID, wc admissionpb.WorkClass,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	handle := s.mu.handles[handleID]
	if handle.wc == wc {
		// Nothing to do, the work class is the same for the handle.
		return
	}

	// Update the work class and move the handle to the new work class watcher.
	handle.wc = wc
	watcher := s.mu.watchers[handle.stc]
	watcher.removeHandleLocked(*handle)
	watcher.tracked[handle.wc] = append(watcher.tracked[handle.wc], handleID)

	// This is the first token for the work class, counter pair, launch a new
	// watcher task.
	if len(watcher.tracked[handle.wc]) == 1 {
		s.watchTokens(context.Background(), handle.stc, wc, watcher)
	}
}

// CancelHandle cancels the given handle, stopping it from being notified
// when tokens are available.
func (s *storeStreamSendTokensWatcher) CancelHandle(handleID StoreStreamSendTokenHandleID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	handle, ok := s.mu.handles[handleID]
	if !ok {
		panic(fmt.Sprintf("handle with handleID=%v does not exist", handleID))
	}
	watcher := s.mu.watchers[handle.stc]
	watcher.removeHandleLocked(*handle)
	delete(s.mu.handles, handle.id)
}

func (s *storeStreamSendTokensWatcher) watchTokens(
	ctx context.Context, stc TokenCounter, wc admissionpb.WorkClass, watcher *tokenWatcher,
) {
	// TODO: This async task should be spawned by the caller. Consider
	// maintaining all the state needed to run the task within a struct,
	// including the notification corresponding to the handleID.
	_ = s.stopper.RunAsyncTask(ctx, "store-stream-token-watcher", func(ctx context.Context) {
		for {
			// Check whether there are no more watchers for the given work class. If
			// so, there's nothing left to do.
			if exit := func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()

				return len(watcher.tracked[wc]) == 0
			}(); exit {
				return
			}

			available, handle := stc.TokensAvailable(wc)
			// If there are no tokens available, we wait here on the handle's wait
			// channel, or until cancelled.
			if !available {
			waiting:
				for {
					select {
					case <-ctx.Done():
						return
					case <-s.stopper.ShouldQuiesce():
						return
					case <-handle.WaitChannel():
						if _, haveTokens := handle.TryDeductAndUnblockNextWaiter(0 /* tokens */); haveTokens {
							break waiting
						}
					}
				}
			}

			if nextNotification := func() TokenAvailableNotification {
				s.mu.Lock()
				defer s.mu.Unlock()

				// There are no more watchers for the given work class, so we're done.
				if len(watcher.tracked[wc]) == 0 {
					return nil
				}

				// Move the next handle to the end of the queue, so that we can rotate
				// through each of the watchers when tokens are available.
				//
				// TODO(kvoli): Should we be using a queue here instead of a slice?
				next := watcher.tracked[wc][0]
				watcher.tracked[wc] = watcher.tracked[wc][1:]
				watcher.tracked[wc] = append(watcher.tracked[wc], next)
				return s.mu.handles[next].notification
			}(); nextNotification != nil {
				nextNotification.Notify()
			}
		}
	})
}
