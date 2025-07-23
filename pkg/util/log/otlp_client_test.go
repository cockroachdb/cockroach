// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
	collpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type mockOTLPServerGRPC struct {
	collpb.UnimplementedLogsServiceServer
	request chan *collpb.ExportLogsServiceRequest
}

func (s *mockOTLPServerGRPC) Export(
	ctx context.Context, req *collpb.ExportLogsServiceRequest,
) (*collpb.ExportLogsServiceResponse, error) {
	s.request <- req
	return &collpb.ExportLogsServiceResponse{}, nil
}

func setupMockOTLPServiceGRPC(
	t testing.TB,
) (string, func(), chan *collpb.ExportLogsServiceRequest) {
	server := grpc.NewServer()
	mock := &mockOTLPServerGRPC{
		request: make(chan *collpb.ExportLogsServiceRequest),
	}
	collpb.RegisterLogsServiceServer(server, mock)

	lis, err := net.ListenTCP("tcp", nil)
	require.NoError(t, err)
	go func() {
		require.NoError(t, server.Serve(lis))
	}()

	cleanup := func() {
		server.Stop()
		close(mock.request)
	}

	return lis.Addr().String(), cleanup, mock.request
}

func setupMockOTLPServiceHTTP(
	t testing.TB,
) (string, func(), chan *collpb.ExportLogsServiceRequest) {

	request := make(chan *collpb.ExportLogsServiceRequest)
	handler := func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, r.Header.Get(httputil.ContentTypeHeader), httputil.ProtoContentType)

		var requestBody collpb.ExportLogsServiceRequest
		bodyBytes, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		r.Body.Close()
		// cannot use protoutil.Unmarshal because the .proto is not generated using
		// gogoproto and we cannot change that
		require.NoError(t, proto.Unmarshal(bodyBytes, &requestBody))
		request <- &requestBody

		w.WriteHeader(http.StatusOK)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	cleanup := func() {
		server.Close()
		close(request)
	}

	return server.URL, cleanup, request
}

func TestOTLPSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	modes := map[string]func(t testing.TB) (string, func(), chan *collpb.ExportLogsServiceRequest){
		"grpc": setupMockOTLPServiceGRPC,
		"http": setupMockOTLPServiceHTTP,
	}

	cfg := logconfig.DefaultConfig()
	format := "json"
	cfg.Sinks.OTLPServers = map[string]*logconfig.OTLPSinkConfig{
		"test": {
			Channels: logconfig.SelectChannels(channel.OPS),
			OTLPDefaults: logconfig.OTLPDefaults{
				Compression: &logconfig.NoneCompression,
				CommonSinkConfig: logconfig.CommonSinkConfig{
					Format:    &format,
					Buffering: disabledBufferingCfg,
				},
			},
		},
	}

	parseJSON := func(body string) (map[string]any, error) {
		var data map[string]any
		err := json.Unmarshal([]byte(body), &data)
		if err != nil {
			return nil, err
		}
		return data, nil
	}

	tests := map[string]struct {
		// number of logs the run function will generate
		logCount int
		run      func()
		// checks the requests that the run function will generate
		check func(t *testing.T, reqs []*collpb.ExportLogsServiceRequest)
	}{
		"single_log": {
			logCount: 1,
			run: func() {
				// checks if the request format is correct
				Ops.Infof(context.Background(), "log message")
			},
			check: func(t *testing.T, reqs []*collpb.ExportLogsServiceRequest) {
				require.Len(t, reqs, 1)
				require.Len(t, reqs[0].ResourceLogs, 1)
				require.Len(t, reqs[0].ResourceLogs[0].InstrumentationLibraryLogs, 1)
				require.Len(t, reqs[0].ResourceLogs[0].InstrumentationLibraryLogs[0].Logs, 1)
				logRecord := reqs[0].ResourceLogs[0].InstrumentationLibraryLogs[0].Logs[0]

				data, err := parseJSON(logRecord.Body.GetStringValue())
				require.NoError(t, err)
				require.Equal(t, "log message", data["message"])
				require.Equal(t, "OPS", data["channel"])
				require.Equal(t, "INFO", data["severity"])
			},
		},
		"multiple_logs": {
			logCount: 3,
			run: func() {
				// checks if sink is able to process multiple messages properly
				Ops.Infof(context.Background(), "log message 1")
				Ops.Infof(context.Background(), "log message 2")
				Ops.Infof(context.Background(), "log message 3")
			},
			check: func(t *testing.T, reqs []*collpb.ExportLogsServiceRequest) {
				require.Len(t, reqs, 3)
			},
		},
		"message_severities": {
			logCount: 3,
			run: func() {
				// checks if sink is producing logs with correct severities
				Ops.Infof(context.Background(), "log info")
				Ops.Warningf(context.Background(), "log warning")
				Ops.Errorf(context.Background(), "log error")
			},
			check: func(t *testing.T, reqs []*collpb.ExportLogsServiceRequest) {
				severities := []string{"INFO", "WARNING", "ERROR"}
				for i, sev := range severities {
					logRecord := reqs[i].ResourceLogs[0].InstrumentationLibraryLogs[0].Logs[0]

					data, err := parseJSON(logRecord.Body.GetStringValue())
					require.NoError(t, err)
					require.Equal(t, sev, data["severity"])
				}
			},
		},
	}

	for mode, startMockServer := range modes {
		address, serverCleanup, request := startMockServer(t)
		cfg.Sinks.OTLPServers["test"].Address = address
		cfg.Sinks.OTLPServers["test"].Mode = &mode

		// Derive a full config using the same directory as the
		// TestLogScope.
		require.NoError(t, cfg.Validate(&sc.logDir))

		// Apply the configuration.
		TestingResetActive()
		cleanup, err := ApplyConfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
		require.NoError(t, err)

		for name, test := range tests {
			// collection of all the requests made to the OTLP server
			var requests []*collpb.ExportLogsServiceRequest
			var wg sync.WaitGroup

			var mu syncutil.Mutex
			go func() {
				for range test.logCount {
					data := <-request
					mu.Lock()
					requests = append(requests, data)
					wg.Done()
					mu.Unlock()
				}
			}()

			t.Run(name, func(t *testing.T) {
				wg.Add(test.logCount)
				test.run()
				wg.Wait() // wait for all requests to be processed
				test.check(t, requests)
			})
		}

		serverCleanup()
		cleanup()
	}
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
