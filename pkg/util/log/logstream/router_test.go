// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstream

import (
	context "context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestLogTypeEventRouter_register(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	router := newLogTypeEventRouter()

	ctrl := gomock.NewController(t)
	mock1A := NewMockProcessor(ctrl)
	mock2A := NewMockProcessor(ctrl)
	mock1B := NewMockProcessor(ctrl)
	mock2B := NewMockProcessor(ctrl)
	typeA := log.EventType("typeA")

	typeB := log.EventType("typeB")

	router.register(ctx, typeA, mock1A)
	router.register(ctx, typeA, mock2A)
	func() {
		router.mu.Lock()
		defer router.mu.Unlock()
		typeAProcessors := router.mu.routes[typeA]
		require.Equal(t, []Processor{mock1A, mock2A}, typeAProcessors)
	}()

	router.register(ctx, typeB, mock1B)
	router.register(ctx, typeB, mock2B)
	func() {
		router.mu.Lock()
		defer router.mu.Unlock()
		typeBProcessors := router.mu.routes[typeB]
		require.Equal(t, []Processor{mock1B, mock2B}, typeBProcessors)
	}()
}

func TestLogTypeEventRouter_Process(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	router := newLogTypeEventRouter()

	ctrl := gomock.NewController(t)
	mock1A := NewMockProcessor(ctrl)
	mock2A := NewMockProcessor(ctrl)
	mock1B := NewMockProcessor(ctrl)
	mock2B := NewMockProcessor(ctrl)

	typeA := log.EventType("typeA")
	typeB := log.EventType("typeB")

	router.register(ctx, typeA, mock1A)
	router.register(ctx, typeA, mock2A)
	router.register(ctx, typeB, mock1B)
	router.register(ctx, typeB, mock2B)

	eventPayloadA := "eventA"
	eventPayloadB := "eventB"
	eventA := &TypedEvent{
		eventType: typeA,
		event:     eventPayloadA,
	}
	eventB := &TypedEvent{
		eventType: typeB,
		event:     eventPayloadB,
	}

	mock1A.EXPECT().Process(gomock.Any(), eventPayloadA)
	mock2A.EXPECT().Process(gomock.Any(), eventPayloadA)
	mock1B.EXPECT().Process(gomock.Any(), eventPayloadB)
	mock2B.EXPECT().Process(gomock.Any(), eventPayloadB)

	require.NoError(t, router.Process(ctx, eventA))
	require.NoError(t, router.Process(ctx, eventB))

	t.Run("panics when fed something other than a TypedEvent", func(t *testing.T) {
		require.Panics(t, func() {
			require.NoError(t, router.Process(ctx, "this should panic"))
		})
	})

	t.Run("gracefully handles events for which there's no registered processor", func(t *testing.T) {
		unregisteredEventType := log.EventType("unregistered")
		event := &TypedEvent{
			eventType: unregisteredEventType,
			event:     "event",
		}
		require.NoError(t, router.Process(ctx, event))
	})

	t.Run("returns errors encountered by underlying processors", func(t *testing.T) {
		expErr := errors.New("error!")
		mock1A.EXPECT().Process(gomock.Any(), eventPayloadA).Return(expErr)
		require.Equal(t, expErr, router.Process(ctx, eventA))
	})
}
