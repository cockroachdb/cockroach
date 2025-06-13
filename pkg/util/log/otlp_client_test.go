// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/stretchr/testify/require"
	collpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"

	"google.golang.org/grpc"
)

type mockLogsServiceServer struct {
	collpb.UnimplementedLogsServiceServer
	request chan *collpb.ExportLogsServiceRequest
}

func (s *mockLogsServiceServer) Export(ctx context.Context, req *collpb.ExportLogsServiceRequest) (*collpb.ExportLogsServiceResponse, error) {
	s.request <- req
	return &collpb.ExportLogsServiceResponse{}, nil
}

func TestOtlpClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	server := grpc.NewServer()
	mock := &mockLogsServiceServer{
		request: make(chan *collpb.ExportLogsServiceRequest, 1),
	}
	collpb.RegisterLogsServiceServer(server, mock)

	lis, err := net.ListenTCP("tcp", nil)
	require.NoError(t, err)
	go server.Serve(lis)
	defer server.Stop()

	cfg := logconfig.DefaultConfig()
	zeroBytes := logconfig.ByteSize(0)
	zeroDuration := time.Duration(0)
	tb := true
	format := "json"
	cfg.Sinks.OtlpServers = map[string]*logconfig.OtlpSinkConfig{
		"ops": {
			Address:  lis.Addr().String(),
			Channels: logconfig.SelectChannels(channel.OPS),
			OtlpDefaults: logconfig.OtlpDefaults{
				Insecure: &tb,
				CommonSinkConfig: logconfig.CommonSinkConfig{
					Format: &format,
					Buffering: logconfig.CommonBufferSinkConfigWrapper{
						CommonBufferSinkConfig: logconfig.CommonBufferSinkConfig{
							MaxStaleness:     &zeroDuration,
							FlushTriggerSize: &zeroBytes,
							MaxBufferSize:    &zeroBytes,
						},
					},
				},
			},
		},
	}

	// Derive a full config using the same directory as the
	// TestLogScope.
	require.NoError(t, cfg.Validate(&sc.logDir))

	// Apply the configuration.
	TestingResetActive()
	cleanup, err := ApplyConfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
	require.NoError(t, err)
	defer cleanup()

	// Send a log event on the OPS channel.
	Ops.Infof(context.Background(), "hello world")

	select {
	case requestData := <-mock.request:
		require.Equal(t, 1, len(requestData.ResourceLogs))
		require.Equal(t, 1, len(requestData.ResourceLogs[0].InstrumentationLibraryLogs))
		require.Equal(t, 1, len(requestData.ResourceLogs[0].InstrumentationLibraryLogs[0].Logs))
		logRecord := requestData.ResourceLogs[0].InstrumentationLibraryLogs[0].Logs[0]

		var info map[string]any
		if err := json.Unmarshal([]byte(logRecord.Body.GetStringValue()), &info); err != nil {
			t.Fatalf("unable to decode json: %v", err)
		}
		require.Equal(t, "hello world", info["message"])
	case <-time.After(time.Second * 5):
		t.Fatal("log call exceeded timeout")
	}
}
