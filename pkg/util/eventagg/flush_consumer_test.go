// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventagg

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestLogWriteConsumer(t *testing.T) {
	defer log.Scope(t).Close(t)
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	type intercepted struct {
		log     KeyValueLog
		logMeta log.StructuredMeta
	}
	interceptedLogs := make(map[string]intercepted)

	consumer := NewLogWriteConsumer[string, string]("test")
	consumer.TestKnobs.LogInterceptorFn = func(ctx context.Context, meta log.StructuredMeta, payload any) {
		l := payload.(KeyValueLog)
		interceptedLogs[l.Key.(string)] = intercepted{
			log:     l,
			logMeta: meta,
		}
	}

	meta := FlushMeta{
		Kind:      Windowed,
		StartTime: 100,
		EndTime:   200,
	}
	flushed := map[string]string{
		"hello": "world",
		"good":  "morning",
	}
	consumer.onFlush(ctx, meta, flushed)

	require.Equal(t, len(flushed), len(interceptedLogs))
	for k, v := range flushed {
		actualV, ok := interceptedLogs[k]
		require.True(t, ok)
		require.Equal(t, v, actualV.log.Value.(string))
		require.Equal(t, meta, actualV.log.Metadata)
		require.Equal(t, log.StructuredMeta{EventType: "test"}, actualV.logMeta)
	}
}
