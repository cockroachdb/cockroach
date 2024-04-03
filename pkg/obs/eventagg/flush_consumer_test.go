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
	"encoding/json"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestLogWriteConsumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)
	ctx := context.Background()

	testEventType := log.EventType("test")
	interceptor := NewEventAggKVLogInterceptor(t)
	interceptor.SetEventTypeFilter(testEventType)
	defer log.InterceptWith(ctx, interceptor)()

	consumer := NewLogWriteConsumer[string, string]("test")
	aggInfo := AggInfo{
		Kind:      Windowed,
		StartTime: 100,
		EndTime:   200,
	}
	flushed := map[string]string{
		"hello": "world",
		"good":  "morning",
	}
	consumer.onFlush(ctx, aggInfo, flushed)

	testutils.SucceedsSoon(t, func() error {
		if len(flushed) != interceptor.Count() {
			return errors.New("still waiting for all expected logs to be intercepted")
		}
		return nil
	})
	logs := interceptor.Logs()
	metas := interceptor.LogMetas()
	for i, l := range logs {
		expectedV, ok := flushed[l.Key.(string)]
		require.True(t, ok)
		require.Equal(t, expectedV, l.Value.(string))
		require.Equal(t, aggInfo, l.AggInfo)
		require.Equal(t, log.StructuredMeta{EventType: testEventType}, metas[i])
	}
}

type KVStructuredLogInterceptor struct {
	testState *testing.T

	typeFilter log.EventType
	mu         struct {
		syncutil.Mutex
		// meta[i] is the metadata corresponding to logs[i]
		logs []KeyValueLog
		meta []log.StructuredMeta
	}
}

var _ log.Interceptor = (*KVStructuredLogInterceptor)(nil)

func NewEventAggKVLogInterceptor(t *testing.T) *KVStructuredLogInterceptor {
	out := &KVStructuredLogInterceptor{
		testState: t,
	}
	out.mu.logs = make([]KeyValueLog, 0)
	out.mu.meta = make([]log.StructuredMeta, 0)
	return out
}

func (e *KVStructuredLogInterceptor) Intercept(entry []byte) {
	var logEntry logpb.Entry

	if err := json.Unmarshal(entry, &logEntry); err != nil {
		e.testState.Fatal(err)
	}

	if logEntry.Channel != logpb.Channel_STRUCTURED_EVENTS {
		return
	}

	// The log.Interceptor uses legacy log formats, which for some reason inserts
	// this string in front of the JSON structured, making it un-parsable unless we strip
	// this prefix.
	structuredEntryPrefix := "Structured entry: "
	var structured log.StructuredPayload
	if err := json.Unmarshal([]byte(strings.TrimPrefix(logEntry.Message, structuredEntryPrefix)), &structured); err != nil {
		e.testState.Fatal(err)
	}

	if e.typeFilter != "" && e.typeFilter != structured.Metadata.EventType {
		return
	}

	// Given that the KeyValueLog has fields with type `any`, we need to re-encode the structured
	// to JSON so that we can deserialize once again, this time into the more specific type.
	// The original deserialization treated structured.Payload as a map[string]interface{}, which is
	// clumsy to work with.
	kvLogBytes, err := json.Marshal(structured.Payload)
	if err != nil {
		e.testState.Fatal(err)
	}

	var kvLog KeyValueLog
	err = json.Unmarshal(kvLogBytes, &kvLog)
	if err != nil {
		e.testState.Fatal(err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.logs = append(e.mu.logs, kvLog)
	e.mu.meta = append(e.mu.meta, structured.Metadata)
}

func (e *KVStructuredLogInterceptor) Logs() []KeyValueLog {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.logs
}

func (e *KVStructuredLogInterceptor) LogMetas() []log.StructuredMeta {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.meta
}

func (e *KVStructuredLogInterceptor) Count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.mu.logs)
}

func (e *KVStructuredLogInterceptor) SetEventTypeFilter(logType log.EventType) {
	e.typeFilter = logType
}
