// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eventagg

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/obs/logstream"
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

	interceptor := NewEventAggKVLogInterceptor[string, string](t)
	meta := log.StructuredLogMeta{
		EventType: "dummy",
		Version:   "1.0",
	}
	interceptor.SetEventFilter(meta)
	defer log.InterceptWith(ctx, interceptor)()

	consumer := NewLogWriteConsumer[string, string](meta)
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
		expectedV, ok := flushed[l.Key]
		require.True(t, ok)
		require.Equal(t, expectedV, l.Value)
		require.Equal(t, aggInfo, l.AggInfo)
		require.Equal(t, meta, metas[i])
	}
}

type KVStructuredLogInterceptor[K any, V any] struct {
	testState *testing.T

	typeFilter log.EventType
	mu         struct {
		syncutil.Mutex
		// meta[i] is the metadata corresponding to logs[i]
		logs []KeyValueLog[K, V]
		meta []log.StructuredLogMeta
	}
}

var _ log.Interceptor = (*KVStructuredLogInterceptor[any, any])(nil)

func NewEventAggKVLogInterceptor[K any, V any](t *testing.T) *KVStructuredLogInterceptor[K, V] {
	out := &KVStructuredLogInterceptor[K, V]{
		testState: t,
	}
	out.mu.logs = make([]KeyValueLog[K, V], 0)
	out.mu.meta = make([]log.StructuredLogMeta, 0)
	return out
}

func (e *KVStructuredLogInterceptor[K, V]) Intercept(entry []byte) {
	var logEntry logpb.Entry

	if err := json.Unmarshal(entry, &logEntry); err != nil {
		e.testState.Fatal(err)
	}

	if logEntry.Channel != logpb.Channel_DEV {
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

	var kvLog KeyValueLog[K, V]
	err = json.Unmarshal(kvLogBytes, &kvLog)
	if err != nil {
		e.testState.Fatal(err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.logs = append(e.mu.logs, kvLog)
	e.mu.meta = append(e.mu.meta, structured.Metadata)
}

func (e *KVStructuredLogInterceptor[K, V]) Logs() []KeyValueLog[K, V] {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.logs
}

func (e *KVStructuredLogInterceptor[K, V]) LogMetas() []log.StructuredLogMeta {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.meta
}

func (e *KVStructuredLogInterceptor[K, V]) Count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.mu.logs)
}

func (e *KVStructuredLogInterceptor[K, V]) SetEventFilter(logType log.StructuredLogMeta) {
	e.typeFilter = logType.EventType
}
