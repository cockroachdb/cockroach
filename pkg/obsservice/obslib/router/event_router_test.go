// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package router

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/obsutil"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	otel_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestEventRouter_Consume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var testEventType obspb.EventType = "testeventtype"
	var testErrorEventType obspb.EventType = "testerroreventtype"
	errorConsumer := obsutil.NewTestErrorConsumer(errors.New("test error"))

	tests := []struct {
		name    string
		event   *obspb.Event
		wantErr bool
		errMsg  string
	}{
		{
			name: "routes event",
			event: &obspb.Event{
				Scope: &otel_pb.InstrumentationScope{
					Name:    string(obspb.EventlogEvent),
					Version: "1.0",
				},
			},
		},
		{
			name: "errors with unknown event type",
			event: &obspb.Event{
				Scope: &otel_pb.InstrumentationScope{
					Name:    "unknown type",
					Version: "1.0",
				},
			},
			wantErr: true,
			errMsg:  "router does not know how to route event type",
		},
		{
			name:    "errors with missing instrumentation scope",
			event:   &obspb.Event{},
			wantErr: true,
			errMsg:  "missing event InstrumentationScope",
		},
		{
			name: "passes back consumer errors",
			event: &obspb.Event{
				Scope: &otel_pb.InstrumentationScope{
					Name:    string(testErrorEventType),
					Version: "1.0",
				},
			},
			wantErr: true,
			errMsg:  "test error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantConsumer := obsutil.NewTestCaptureConsumer()
			otherConsumer := obsutil.NewTestCaptureConsumer()
			e := NewEventRouter(map[obspb.EventType]obslib.EventConsumer{
				obspb.EventlogEvent: wantConsumer,
				testEventType:       otherConsumer,
				testErrorEventType:  errorConsumer,
			})
			err := e.Consume(ctx, tc.event)
			if tc.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, wantConsumer.Len(), 1)
			require.Zero(t, otherConsumer.Len())
		})
	}
}
