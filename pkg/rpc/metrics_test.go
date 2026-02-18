// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"storj.io/drpc"
)

// TestMetricsRelease verifies that peerMetrics.release() removes tracking for
// *all* the metrics from their parent aggregate metric.
func TestMetricsRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// All metrics in aggmetric package satisfy this interface. The `Each` method
	// can be used to scan all child metrics of the aggregated metric. We use it
	// for counting children.
	type eacher interface {
		Each([]*io_prometheus_client.LabelPair, func(metric *io_prometheus_client.Metric))
	}
	countChildren := func(metric eacher) (count int) {
		metric.Each(nil /*labels*/, func(*io_prometheus_client.Metric) {
			count++
		})
		return count
	}

	verifyAllFields := func(m *Metrics, wantChildren int) (metricFields int) {
		r := reflect.ValueOf(m).Elem()
		for i, n := 0, r.NumField(); i < n; i++ {
			if !r.Field(i).CanInterface() {
				continue
			}
			field := r.Field(i).Interface()
			metric, ok := field.(eacher)
			if !ok { // skip all non-metric fields
				continue
			}
			metricFields++
			require.Equal(t, wantChildren, countChildren(metric), r.Type().Field(i).Name)
		}
		return metricFields
	}

	const expectedCount = 13
	k1 := peerKey{NodeID: 5, TargetAddr: "192.168.0.1:1234", Class: rpcbase.DefaultClass}
	k2 := peerKey{NodeID: 6, TargetAddr: "192.168.0.1:1234", Class: rpcbase.DefaultClass}
	l1 := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}}
	l2 := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}}
	m := newMetrics(l1)
	// Verify that each metric doesn't have any children at first. Verify the
	// number of metric fields, as a sanity check (to be modified if fields are
	// added/deleted).
	require.Equal(t, expectedCount, verifyAllFields(m, 0))
	// Verify that a new peer's metrics all get registered.
	pm, lm := m.acquire(k1, l1, rpcProtocolGRPC)
	require.Equal(t, expectedCount, verifyAllFields(m, 1))
	// Acquire the same peer. The count remains at 1.
	pm2, lm2 := m.acquire(k1, l1, rpcProtocolGRPC)
	require.Equal(t, expectedCount, verifyAllFields(m, 1))
	require.Equal(t, pm, pm2)
	require.Equal(t, lm, lm2)

	// Acquire a different peer but the same locality.
	pm3, lm3 := m.acquire(k2, l1, rpcProtocolGRPC)
	require.NotEqual(t, pm, pm3)
	require.Equal(t, lm, lm3)

	// Acquire a different locality but the same peer.
	pm4, lm4 := m.acquire(k1, l2, rpcProtocolGRPC)
	require.Equal(t, pm, pm4)
	require.NotEqual(t, lm, lm4)

	// We added one extra peer and one extra locality, verify counts.
	require.Equal(t, expectedCount, verifyAllFields(m, 2))

	// Acquire the same peer and locality with drpc protocol
	pm5, lm5 := m.acquire(k1, l1, rpcProtocolDRPC)
	require.NotEqual(t, pm, pm5)
	require.Equal(t, lm, lm5)
}

func TestDRPCServerRequestInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	req := struct{}{}

	testcase := []struct {
		service      string
		methodName   string
		statusCode   codes.Code
		shouldRecord bool
	}{
		{"testservice", "rpc/test/method", codes.OK, true},
		{"testservice", "rpc/test/method", codes.Internal, true},
		{"testservice", "rpc/test/method", codes.Aborted, true},
		{"testservice", "rpc/test/notRecorded", codes.OK, false},
	}

	for _, tc := range testcase {
		t.Run(fmt.Sprintf("%s %s", tc.methodName, tc.statusCode),
			func(t *testing.T) {
				requestMetrics := NewDRPCServerRequestMetrics()
				reportable := &Reportable{ServerMetrics: requestMetrics}
				rpc := "/" + tc.service + "/" + tc.methodName
				handler := func(ctx context.Context, req interface{}) (interface{}, error) {
					if tc.statusCode == codes.OK {
						time.Sleep(time.Millisecond)
						return struct{}{}, nil
					}
					return nil, status.Error(tc.statusCode, tc.statusCode.String())
				}
				interceptor := NewDRPCUnaryServerRequestMetricsInterceptor(reportable, func(fullMethodName string) bool {
					return tc.shouldRecord
				})
				_, err := interceptor(ctx, req, rpc, handler)
				if err != nil {
					require.Equal(t, tc.statusCode, status.Code(err))
				}
				var expectedCount uint64
				if tc.shouldRecord {
					expectedCount = 1
				}
				assertRpcMetrics(t, requestMetrics.ServerHandledDuration.ToPrometheusMetrics(), map[string]uint64{
					fmt.Sprintf("%s %s", tc.methodName, tc.statusCode): expectedCount,
				})
			})
	}
}

func TestServerRequestInstrumentInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	requestMetrics := NewRequestMetrics()

	ctx := context.Background()
	req := struct{}{}

	testcase := []struct {
		methodName   string
		statusCode   codes.Code
		shouldRecord bool
	}{
		{"rpc/test/method", codes.OK, true},
		{"rpc/test/method", codes.Internal, true},
		{"rpc/test/method", codes.Aborted, true},
		{"rpc/test/notRecorded", codes.OK, false},
	}

	for _, tc := range testcase {
		t.Run(fmt.Sprintf("%s %s", tc.methodName, tc.statusCode),
			func(t *testing.T) {
				info := &grpc.UnaryServerInfo{FullMethod: tc.methodName}
				handler := func(ctx context.Context, req interface{}) (interface{}, error) {
					if tc.statusCode == codes.OK {
						time.Sleep(time.Millisecond)
						return struct{}{}, nil
					}
					return nil, status.Error(tc.statusCode, tc.statusCode.String())
				}
				interceptor := NewRequestMetricsInterceptor(requestMetrics, func(fullMethodName string) bool {
					return tc.shouldRecord
				})
				_, err := interceptor(ctx, req, info, handler)
				if err != nil {
					require.Equal(t, tc.statusCode, status.Code(err))
				}
				var expectedCount uint64
				if tc.shouldRecord {
					expectedCount = 1
				}
				assertRpcMetrics(t, requestMetrics.Duration.ToPrometheusMetrics(), map[string]uint64{
					fmt.Sprintf("%s %s", tc.methodName, tc.statusCode): expectedCount,
				})
			})
	}
}

func TestGatewayRequestRecoveryInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// With gateway metadata - should recover from panic
	t.Run("with gateway metadata", func(t *testing.T) {
		// Create a context with the gateway metadata
		md := metadata.New(map[string]string{
			gwRequestKey: "test",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		// Create a handler that panics
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			panic("test panic")
		}

		// Call the interceptor
		resp, err := gatewayRequestRecoveryInterceptor(ctx, nil, nil, handler)

		// Verify the panic was recovered and converted to an error
		require.Nil(t, resp)
		require.ErrorContains(t, err, "unexpected error occurred")
	})

	// Without gateway metadata - should not recover from panic
	t.Run("without gateway metadata", func(t *testing.T) {
		// Create a context without the gateway metadata
		ctx := context.Background()

		// Create a handler that panics
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			panic("test panic")
		}

		// Call the interceptor and expect it to panic
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to propagate, got none")
			}
		}()

		_, _ = gatewayRequestRecoveryInterceptor(ctx, nil, nil, handler)
	})

	// With gateway metadata but no panic - should pass through normally
	t.Run("with gateway metadata no panic", func(t *testing.T) {
		// Create a context with the gateway metadata
		md := metadata.New(map[string]string{
			gwRequestKey: "test",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		// Create a handler that returns normally
		expectedResp := "success"
		expectedErr := errors.New("expected error")
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return expectedResp, expectedErr
		}

		// Call the interceptor
		resp, err := gatewayRequestRecoveryInterceptor(ctx, nil, nil, handler)

		// Verify the response and error were passed through unchanged
		require.Equal(t, expectedResp, resp)
		require.ErrorIs(t, err, expectedErr)
	})
}

func TestSplitDRPCMethod(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := []struct {
		input       string
		wantService string
		wantMethod  string
	}{
		{"/cockroach.rpc.Heartbeat/Ping", "cockroach.rpc.Heartbeat", "Ping"},
		{"/service/method", "service", "method"},
		{"service/method", "service", "method"},
		{"/service/nested/method", "service", "nested/method"},
		{"noslash", "unknown", "unknown"},
		{"", "unknown", "unknown"},
	}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			service, method := splitDRPCMethod(tc.input)
			require.Equal(t, tc.wantService, service)
			require.Equal(t, tc.wantMethod, method)
		})
	}
}

func TestDRPCServerUnaryInterceptorAllMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	requestMetrics := NewDRPCServerRequestMetrics()
	reportable := &Reportable{ServerMetrics: requestMetrics}

	ctx := context.Background()
	req := struct{}{}

	rpc := "/testservice/TestMethod"
	interceptor := NewDRPCUnaryServerRequestMetricsInterceptor(
		reportable, func(string) bool { return true })

	// Successful call.
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		time.Sleep(time.Millisecond)
		return struct{}{}, nil
	}
	_, err := interceptor(ctx, req, rpc, handler)
	require.NoError(t, err)

	baseLabels := map[string]string{
		RPCTypeLabel:    "unary",
		RPCServiceLabel: "testservice",
		RPCMethodLabel:  "TestMethod",
	}
	codeLabelsOK := map[string]string{
		RPCTypeLabel:       "unary",
		RPCServiceLabel:    "testservice",
		RPCMethodLabel:     "TestMethod",
		RPCStatusCodeLabel: "OK",
	}

	require.Equal(t, int64(1), requestMetrics.ServerStarted.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerHandled.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerMsgReceived.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerMsgSent.Count(baseLabels))
	require.Equal(t, int64(0), requestMetrics.ServerErrors.Count(codeLabelsOK))

	// Error call.
	errHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, status.Error(codes.Internal, "internal error")
	}
	_, err = interceptor(ctx, req, rpc, errHandler)
	require.Error(t, err)

	codeLabelsInternal := map[string]string{
		RPCTypeLabel:       "unary",
		RPCServiceLabel:    "testservice",
		RPCMethodLabel:     "TestMethod",
		RPCStatusCodeLabel: "Internal",
	}

	require.Equal(t, int64(2), requestMetrics.ServerStarted.Count(baseLabels))
	require.Equal(t, int64(2), requestMetrics.ServerHandled.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerErrors.Count(codeLabelsInternal))
}

// mockDRPCStream is a minimal drpc.Stream implementation for testing.
type mockDRPCStream struct {
	ctx context.Context
}

var _ drpc.Stream = (*mockDRPCStream)(nil)

func (s *mockDRPCStream) Context() context.Context                  { return s.ctx }
func (s *mockDRPCStream) MsgSend(drpc.Message, drpc.Encoding) error { return nil }
func (s *mockDRPCStream) MsgRecv(drpc.Message, drpc.Encoding) error { return nil }
func (s *mockDRPCStream) CloseSend() error                          { return nil }
func (s *mockDRPCStream) Close() error                              { return nil }

func TestDRPCStreamServerRequestInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	requestMetrics := NewDRPCServerRequestMetrics()
	reportable := &Reportable{ServerMetrics: requestMetrics}

	rpc := "/testservice/StreamMethod"
	interceptor := NewDRPCStreamServerRequestMetricsInterceptor(
		reportable, func(string) bool { return true })

	baseLabels := map[string]string{
		RPCTypeLabel:    "stream",
		RPCServiceLabel: "testservice",
		RPCMethodLabel:  "StreamMethod",
	}

	// Successful stream call. The handler receives a monitoredDRPCServerStream
	// and calls MsgRecv + MsgSend on it to exercise the wrappers.
	stream := &mockDRPCStream{ctx: context.Background()}
	_, err := interceptor(stream, rpc, func(s drpc.Stream) (interface{}, error) {
		// Simulate receiving and sending one message each.
		if err := s.MsgRecv(nil, nil); err != nil {
			return nil, err
		}
		if err := s.MsgSend(nil, nil); err != nil {
			return nil, err
		}
		return "ok", nil
	})
	require.NoError(t, err)

	require.Equal(t, int64(1), requestMetrics.ServerStarted.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerHandled.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerMsgReceived.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerMsgSent.Count(baseLabels))

	// Error stream call.
	stream2 := &mockDRPCStream{ctx: context.Background()}
	_, err = interceptor(stream2, rpc, func(s drpc.Stream) (interface{}, error) {
		return nil, status.Error(codes.Unavailable, "unavailable")
	})
	require.Error(t, err)

	codeLabelsUnavailable := map[string]string{
		RPCTypeLabel:       "stream",
		RPCServiceLabel:    "testservice",
		RPCMethodLabel:     "StreamMethod",
		RPCStatusCodeLabel: "Unavailable",
	}

	require.Equal(t, int64(2), requestMetrics.ServerStarted.Count(baseLabels))
	require.Equal(t, int64(2), requestMetrics.ServerHandled.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerErrors.Count(codeLabelsUnavailable))
	// MsgReceived/MsgSent should not have incremented from the error call.
	require.Equal(t, int64(1), requestMetrics.ServerMsgReceived.Count(baseLabels))
	require.Equal(t, int64(1), requestMetrics.ServerMsgSent.Count(baseLabels))

	// shouldRecord=false: no metrics recorded.
	noRecordInterceptor := NewDRPCStreamServerRequestMetricsInterceptor(
		reportable, func(string) bool { return false })
	stream3 := &mockDRPCStream{ctx: context.Background()}
	_, err = noRecordInterceptor(stream3, rpc, func(s drpc.Stream) (interface{}, error) {
		return "skipped", nil
	})
	require.NoError(t, err)
	// Counts should not have changed.
	require.Equal(t, int64(2), requestMetrics.ServerStarted.Count(baseLabels))
}

func assertRpcMetrics(
	t *testing.T, metrics []*io_prometheus_client.Metric, expected map[string]uint64,
) {
	t.Helper()
	actual := map[string]*io_prometheus_client.Histogram{}
	for _, m := range metrics {
		var method, statusCode string
		for _, l := range m.Label {
			switch *l.Name {
			case RPCMethodLabel:
				method = *l.Value
			case RPCStatusCodeLabel:
				statusCode = *l.Value
			}
		}
		histogram := m.Histogram
		require.NotNil(t, histogram, "expected histogram")
		key := fmt.Sprintf("%s %s", method, statusCode)
		actual[key] = histogram
	}

	for key, val := range expected {
		histogram, ok := actual[key]
		if val == 0 {
			require.False(t, ok, "expected `%s` to not exist", key)
		} else {
			require.True(t, ok)
			require.Greater(t, *histogram.SampleSum, float64(0), "expected `%s` to have a SampleSum > 0", key)
			require.Equal(t, val, *histogram.SampleCount, "expected `%s` to have SampleCount of %d", key, val)
		}
	}
}
