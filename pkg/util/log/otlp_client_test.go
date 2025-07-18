// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	collpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	cpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc"
)

type mockLogsServiceServer struct {
	collpb.UnimplementedLogsServiceServer
	request chan *collpb.ExportLogsServiceRequest
}

func (s *mockLogsServiceServer) Export(
	ctx context.Context, req *collpb.ExportLogsServiceRequest,
) (*collpb.ExportLogsServiceResponse, error) {
	s.request <- req
	return &collpb.ExportLogsServiceResponse{}, nil
}

func setupMockOTLPLogService(
	t testing.TB,
) (address string, cleanup func(), request chan *collpb.ExportLogsServiceRequest) {
	server := grpc.NewServer()
	mock := &mockLogsServiceServer{
		request: make(chan *collpb.ExportLogsServiceRequest, 1),
	}
	collpb.RegisterLogsServiceServer(server, mock)

	lis, err := net.ListenTCP("tcp", nil)
	require.NoError(t, err)
	go func() {
		require.NoError(t, server.Serve(lis))
	}()

	cleanup = func() {
		server.Stop()
		close(mock.request)
	}

	return lis.Addr().String(), cleanup, mock.request
}

func TestOTLPClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	address, cleanup, request := setupMockOTLPLogService(t)
	defer cleanup()

	cfg := logconfig.DefaultConfig()
	zeroBytes := logconfig.ByteSize(0)
	zeroDuration := time.Duration(0)
	format := "json"
	cfg.Sinks.OTLPServers = map[string]*logconfig.OTLPSinkConfig{
		"ops": {
			Address:  address,
			Channels: logconfig.SelectChannels(channel.OPS),
			OTLPDefaults: logconfig.OTLPDefaults{
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
	case requestData := <-request:
		require.Equal(t, 1, len(requestData.ResourceLogs))
		require.Equal(t, 1, len(requestData.ResourceLogs[0].InstrumentationLibraryLogs))
		require.Equal(t, 1, len(requestData.ResourceLogs[0].InstrumentationLibraryLogs[0].Logs))
		logRecord := requestData.ResourceLogs[0].InstrumentationLibraryLogs[0].Logs[0]

		var info map[string]any
		if err := json.Unmarshal([]byte(logRecord.Body.GetStringValue()), &info); err != nil {
			t.Fatalf("unable to decode json: %v", err)
		}
		require.Equal(t, "hello world", info["message"])
		require.Equal(t, "OPS", info["channel"])
		require.Equal(t, "INFO", info["severity"])
	case <-time.After(time.Second * 5):
		t.Fatal("log call exceeded timeout")
	}
}

func TestOTLPClientSeverity(t *testing.T) {
	defer build.TestingOverrideVersion("v999.0.0")()
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	address, cleanup, request := setupMockOTLPLogService(t)
	defer cleanup()

	cfg := logconfig.DefaultConfig()
	zeroBytes := logconfig.ByteSize(0)
	zeroDuration := time.Duration(0)
	format := "json"
	cfg.Sinks.OTLPServers = map[string]*logconfig.OTLPSinkConfig{
		"ops": {
			Address:  address,
			Channels: logconfig.SelectChannels(channel.OPS),
			OTLPDefaults: logconfig.OTLPDefaults{
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
	cleanup, err := ApplyConfig(cfg, nil, nil)
	require.NoError(t, err)
	defer cleanup()

	// modifies indeterministic fields like timestamp, goroutine, and line number from the request
	removeIndeterministicFields := func(request *collpb.ExportLogsServiceRequest) error {
		logs := request.ResourceLogs[0].InstrumentationLibraryLogs[0].Logs
		for _, log := range logs {
			var info map[string]any
			if err := json.Unmarshal([]byte(log.Body.GetStringValue()), &info); err != nil {
				return err
			}
			info["timestamp"] = "1337"
			info["goroutine"] = 42
			info["line"] = 123
			newBody, err := json.Marshal(info)
			if err != nil {
				return err
			}
			log.Body = &cpb.AnyValue{
				Value: &cpb.AnyValue_StringValue{
					StringValue: string(newBody),
				},
			}
		}
		return nil
	}

	datadriven.RunTest(t, "testdata/otlp_sink", func(t *testing.T, td *datadriven.TestData) string {
		var output []string
		var mu syncutil.Mutex
		var wg sync.WaitGroup

		go func() {
			for requestData := range request {
				err := removeIndeterministicFields(requestData)
				require.NoError(t, err)
				// the whitespaces seem to differ between environments
				// so this is a quick fix to ensure consistent output
				message := strings.ReplaceAll(requestData.String(), "  ", " ")
				mu.Lock()
				output = append(output, message)
				wg.Done()
				mu.Unlock()
			}
		}()

		ctx := context.Background()
		wg.Add(3)
		Ops.Info(ctx, "log message")
		Ops.Warning(ctx, "log message")
		Ops.Error(ctx, "log message")
		wg.Wait()

		return strings.Join(output, "\n")
	})
}

func TestOTLPExtractRecords(t *testing.T) {
	tests := map[string]struct {
		input  string
		result []string
	}{
		"single_line": {
			input:  "Hello World",
			result: []string{"Hello World"},
		},
		"multiple_lines": {
			input:  "Message 1\nMessage 2\n\nMessage 3",
			result: []string{"Message 1", "Message 2", "Message 3"},
		},
		"trailing_newline": {
			input:  "Message 1\nMessage 2\n",
			result: []string{"Message 1", "Message 2"},
		},
		"leading_newline": {
			input:  "\nMessage 1\nMessage 2",
			result: []string{"Message 1", "Message 2"},
		},
		"redaction_markers": {
			input:  "Message 1\n‹Message 2›\nMessage 3",
			result: []string{"Message 1", "‹Message 2›", "Message 3"},
		},
		"empty_string": {
			input:  "",
			result: []string{},
		},
		"newline_only": {
			input:  "\n\n",
			result: []string{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			body := []byte(test.input)
			records := otlpExtractRecords(body)
			require.Len(t, records, len(test.result))
			for i, record := range records {
				require.Equal(t, test.result[i], record.Body.GetStringValue())
			}
		})
	}
}
