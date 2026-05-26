// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDetectArtifact(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK\x03\x04fake-zip-content"), 0644))

	notAZipPath := filepath.Join(tmpDir, "notazip.bin")
	require.NoError(t, os.WriteFile(notAZipPath, []byte("not-a-zip-file"), 0644))

	emptyPath := filepath.Join(tmpDir, "empty.zip")
	require.NoError(t, os.WriteFile(emptyPath, nil, 0644))

	dirPath := filepath.Join(tmpDir, "debug-dir")
	require.NoError(t, os.Mkdir(dirPath, 0755))

	tests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{name: "valid zip", input: zipPath},
		{name: "directory rejected", input: dirPath, expectedErr: "is a directory"},
		{name: "empty file rejected", input: emptyPath, expectedErr: "is empty"},
		{name: "non-zip rejected", input: notAZipPath, expectedErr: "not a supported artifact"},
		{name: "missing file", input: filepath.Join(tmpDir, "nope.zip"), expectedErr: "cannot access"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := detectArtifact(tt.input)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateUploadConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	origOpts := debugUploadOpts
	defer func() { debugUploadOpts = origOpts }()

	tests := []struct {
		name        string
		apiKey      string
		url         string
		expectedErr string
	}{
		{name: "both set", apiKey: "key", url: "https://example.com"},
		{name: "missing api key", url: "https://example.com", expectedErr: "--crl-support-api-key is required"},
		{name: "missing url", apiKey: "key", expectedErr: "--crl-support-url is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			debugUploadOpts.crlSupportAPIKey = tt.apiKey
			debugUploadOpts.crlSupportURL = tt.url
			err := validateUploadConfig()
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewUploadHTTPClientProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	origOpts := debugUploadOpts
	defer func() { debugUploadOpts = origOpts }()

	t.Run("invalid proxy URL fails fast", func(t *testing.T) {
		debugUploadOpts.proxy = "://not-a-url"
		_, err := newUploadHTTPClient(crlSupportTimeout)
		require.Error(t, err)
	})

	t.Run("valid proxy URL is set on transport", func(t *testing.T) {
		debugUploadOpts.proxy = "http://proxy.example.com:3128"
		client, err := newUploadHTTPClient(crlSupportTimeout)
		require.NoError(t, err)

		transport, ok := client.Transport.(*http.Transport)
		require.True(t, ok)
		require.NotNil(t, transport.Proxy)

		req, err := http.NewRequest(http.MethodGet, "https://upload.example/", nil)
		require.NoError(t, err)
		proxyURL, err := transport.Proxy(req)
		require.NoError(t, err)
		require.NotNil(t, proxyURL)
		require.Equal(t, "proxy.example.com:3128", proxyURL.Host)
	})
}

// TestNewUploadHTTPClientHTTPSProxyEnvVar verifies the env-var fallback
// and the precedence rule (--proxy wins over $HTTPS_PROXY).
func TestNewUploadHTTPClientHTTPSProxyEnvVar(t *testing.T) {
	defer leaktest.AfterTest(t)()

	origOpts := debugUploadOpts
	defer func() { debugUploadOpts = origOpts }()

	proxyForReq := func(c *http.Client) string {
		t.Helper()
		transport, ok := c.Transport.(*http.Transport)
		require.True(t, ok)
		require.NotNil(t, transport.Proxy)
		req, err := http.NewRequest(http.MethodGet, "https://upload.example/", nil)
		require.NoError(t, err)
		u, err := transport.Proxy(req)
		require.NoError(t, err)
		if u == nil {
			return ""
		}
		return u.Host
	}

	t.Run("env var honoured when --proxy unset", func(t *testing.T) {
		debugUploadOpts.proxy = ""
		t.Setenv("HTTPS_PROXY", "http://env-proxy.example:8080")
		c, err := newUploadHTTPClient(crlSupportTimeout)
		require.NoError(t, err)
		require.Equal(t, "env-proxy.example:8080", proxyForReq(c))
	})

	t.Run("--proxy wins over env var", func(t *testing.T) {
		debugUploadOpts.proxy = "http://flag-proxy.example:1234"
		t.Setenv("HTTPS_PROXY", "http://env-proxy.example:5678")
		c, err := newUploadHTTPClient(crlSupportTimeout)
		require.NoError(t, err)
		require.Equal(t, "flag-proxy.example:1234", proxyForReq(c))
	})
}

// TestCreateSessionWireFormat exercises createSession against a
// real HTTP server, asserting URL, method, headers, request body
// (mode + labels.ticket_id), and JSON unmarshaling of the response.
func TestCreateSessionWireFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		gotMethod, gotPath, gotAuth, gotCT string
		gotBody                            map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"session_id":"ses_x","upload_token":"tok_y"}`))
	}))
	defer srv.Close()

	c := newCRLSupportClient(srv.URL, srv.Client())
	id, tok, err := c.createSession(context.Background(), "the-api-key", "TICKET-123")
	require.NoError(t, err)
	require.Equal(t, "ses_x", id)
	require.Equal(t, "tok_y", tok)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/api/v1/sessions", gotPath)
	require.Equal(t, "Bearer the-api-key", gotAuth)
	require.Equal(t, "application/json", gotCT)
	require.Equal(t, "direct", gotBody["mode"])
	labels, ok := gotBody["labels"].(map[string]any)
	require.True(t, ok, "labels should be a JSON object, got %T", gotBody["labels"])
	require.Equal(t, "TICKET-123", labels["ticket_id"])
}

// TestCreateSessionOmitsLabelsWhenNoTicketID verifies that the labels
// field is left out of the request body entirely when no ticket-id is
// supplied.
func TestCreateSessionOmitsLabelsWhenNoTicketID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"session_id":"s","upload_token":"t"}`))
	}))
	defer srv.Close()

	c := newCRLSupportClient(srv.URL, srv.Client())
	_, _, err := c.createSession(context.Background(), "key", "")
	require.NoError(t, err)
	require.Equal(t, "direct", gotBody["mode"])
	_, hasLabels := gotBody["labels"]
	require.False(t, hasLabels, "labels must be omitted when no ticket-id is set, got %v", gotBody["labels"])
}

// TestCreateSessionNonCreatedReturnsError verifies that any non-201
// response surfaces as a wrapped error containing the status and body.
func TestCreateSessionNonCreatedReturnsError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("invalid api key"))
	}))
	defer srv.Close()

	c := newCRLSupportClient(srv.URL, srv.Client())
	_, _, err := c.createSession(context.Background(), "bad", "")
	require.ErrorContains(t, err, "session creation failed (401)")
	require.ErrorContains(t, err, "invalid api key")
}

// TestReopenSessionUsesAPIKeyAuth pins down the contract that
// /reupload authenticates with the cluster API key, not the upload
// token (the whole point of this endpoint).
func TestReopenSessionUsesAPIKeyAuth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		gotMethod, gotPath, gotAuth string
		gotBody                     map[string]string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"session_id":"ses_old","upload_token":"tok_new"}`))
	}))
	defer srv.Close()

	c := newCRLSupportClient(srv.URL, srv.Client())
	id, tok, err := c.reopenSession(context.Background(), "ses_old", "the-api-key")
	require.NoError(t, err)
	require.Equal(t, "ses_old", id)
	require.Equal(t, "tok_new", tok)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/api/v1/sessions/ses_old/reupload", gotPath)
	require.Equal(t, "Bearer the-api-key", gotAuth)
	require.NotEmpty(t, gotBody["reason"])
}

// TestCreateResumableSessionAcceptsBoth200And201 verifies that both
// "freshly minted" (201) and "cached reuse" (200) are treated as
// success. Also pins down that the request body carries only
// content_type — object_name is not sent because the server picks it.
func TestCreateResumableSessionAcceptsBoth200And201(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name   string
		status int
		reused bool
	}{
		{name: "freshly minted", status: http.StatusCreated, reused: false},
		{name: "cached reuse", status: http.StatusOK, reused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				gotAuth string
				gotBody map[string]any
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAuth = r.Header.Get("Authorization")
				require.Equal(t, "/api/v1/sessions/ses_z/resumable-session", r.URL.Path)
				require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
				w.WriteHeader(tc.status)
				body := map[string]any{
					"gcs_session_uri": "https://gcs.example/upload?id=fake",
					"bucket":          "b",
					"object_path":     "ses_z/debug.zip",
					"content_type":    "application/zip",
					"reused":          tc.reused,
				}
				require.NoError(t, json.NewEncoder(w).Encode(body))
			}))
			defer srv.Close()

			c := newCRLSupportClient(srv.URL, srv.Client())
			uri, bucket, op, ct, err := c.createResumableSession(
				context.Background(), "ses_z", "tok", "application/zip",
			)
			require.NoError(t, err)
			require.Equal(t, "https://gcs.example/upload?id=fake", uri)
			require.Equal(t, "b", bucket)
			require.Equal(t, "ses_z/debug.zip", op)
			require.Equal(t, "application/zip", ct)
			require.Equal(t, "Bearer tok", gotAuth, "must use upload token, not API key")
			require.Equal(t, "application/zip", gotBody["content_type"])
			_, hasObjectName := gotBody["object_name"]
			require.False(t, hasObjectName, "client must not send object_name; server controls naming")
		})
	}
}

// TestCompleteUploadSessionWireFormat pins down the /complete
// contract: POST with status="success" and artifacts_uploaded=1.
func TestCompleteUploadSessionWireFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		gotMethod, gotPath, gotAuth, gotCT string
		gotBody                            map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newCRLSupportClient(srv.URL, srv.Client())
	require.NoError(t, completeUploadSession(
		context.Background(), c, "ses_a", "tok_b",
	))
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/api/v1/sessions/ses_a/complete", gotPath)
	require.Equal(t, "Bearer tok_b", gotAuth)
	require.Equal(t, "application/json", gotCT)
	require.Equal(t, "success", gotBody["status"])
	require.EqualValues(t, 1, gotBody["artifacts_uploaded"])
}

// uploadScenario configures the mock responses for one datadriven case.
type uploadScenario struct {
	TicketID      string `json:"ticket_id"`
	ResumeSession string `json:"resume_session"`

	CreateSession    *endpointResponse  `json:"create_session"`
	Reupload         *endpointResponse  `json:"reupload"`
	ResumableSession *endpointResponse  `json:"resumable_session"`
	BlobProbe        *endpointResponse  `json:"blob_probe"`
	BlobPut          []endpointResponse `json:"blob_put"`
	Complete         *endpointResponse  `json:"complete"`
}

// endpointResponse mocks one HTTP response. Set Body for raw replies
// (typically errors); otherwise the harness builds the body from the
// structured fields below.
type endpointResponse struct {
	Status int    `json:"status"`
	Body   string `json:"body,omitempty"`

	// createSession / reupload.
	SessionID   string `json:"session_id,omitempty"`
	UploadToken string `json:"upload_token,omitempty"`

	// resumable_session.
	ObjectPath string `json:"object_path,omitempty"`
	Bucket     string `json:"bucket,omitempty"`

	// blob_probe 308: already-uploaded offset, sent as Range: bytes=0-<RangeEnd>.
	RangeEnd int64 `json:"range_end,omitempty"`
}

// mockCRLServerHost is synthetic: doCRLHTTPRequest is hooked, so no
// real server listens on it.
const mockCRLServerHost = "http://crl-support.test"

func TestCRLSupportUploadDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/debug_upload", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			require.Equal(t, "upload", d.Cmd, "only the 'upload' directive is supported")
			var s uploadScenario
			require.NoError(t, json.Unmarshal([]byte(d.Input), &s),
				"invalid scenario JSON")
			return runUploadScenario(t, s)
		})
	})
}

// scenarioCounters track per-endpoint hit counts; included in the
// captured output so scenarios can assert on them.
type scenarioCounters struct {
	createSession    atomic.Int32
	reupload         atomic.Int32
	resumableSession atomic.Int32
	complete         atomic.Int32
	blobProbe        atomic.Int32
	blobPut          atomic.Int32
}

func runUploadScenario(t *testing.T, s uploadScenario) string {
	t.Helper()

	var ctr scenarioCounters

	defer testutils.TestingHook(&doCRLHTTPRequest,
		func(_ *http.Client, req *http.Request) (*http.Response, error) {
			switch {
			case req.URL.Path == "/api/v1/sessions":
				ctr.createSession.Add(1)
				require.NotNil(t, s.CreateSession, "scenario missing create_session response")
				return buildSessionJSONResponse(req, s.CreateSession), nil

			case strings.HasSuffix(req.URL.Path, "/reupload"):
				ctr.reupload.Add(1)
				require.NotNil(t, s.Reupload, "scenario missing reupload response")
				return buildSessionJSONResponse(req, s.Reupload), nil

			case strings.HasSuffix(req.URL.Path, "/resumable-session"):
				ctr.resumableSession.Add(1)
				require.NotNil(t, s.ResumableSession,
					"scenario missing resumable_session response")
				// Blob URI loops back to our mock host so the PUT also lands here.
				blobURI := mockCRLServerHost + "/blob/" + s.ResumableSession.ObjectPath
				return buildResumableSessionResponse(req, s.ResumableSession, blobURI), nil

			case strings.HasSuffix(req.URL.Path, "/complete"):
				ctr.complete.Add(1)
				require.NotNil(t, s.Complete, "scenario missing complete response")
				return buildRawResponse(req, s.Complete), nil

			case strings.HasPrefix(req.URL.Path, "/blob/"):
				if strings.HasPrefix(req.Header.Get("Content-Range"), "bytes */") {
					ctr.blobProbe.Add(1)
					require.NotNil(t, s.BlobProbe,
						"scenario missing blob_probe response (resume flow probes the URI)")
					return buildBlobProbeResponse(req, s.BlobProbe), nil
				}
				i := int(ctr.blobPut.Add(1)) - 1
				require.Less(t, i, len(s.BlobPut),
					"blob PUT attempt %d but only %d responses configured", i+1, len(s.BlobPut))
				return buildRawResponse(req, &s.BlobPut[i]), nil

			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
				return nil, nil
			}
		},
	)()

	origOpts := debugUploadOpts
	t.Cleanup(func() { debugUploadOpts = origOpts })

	debugUploadOpts.crlSupportAPIKey = "test-api-key"
	debugUploadOpts.crlSupportURL = mockCRLServerHost
	debugUploadOpts.crlSupportTicketID = s.TicketID
	debugUploadOpts.resumeSession = s.ResumeSession
	debugUploadOpts.proxy = ""
	t.Setenv(httpsProxyEnvVar, "") // prevent host env leakage

	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "debug.zip")
	require.NoError(t, os.WriteFile(zipPath, []byte("PK\x03\x04fake-zip-content"), 0644))

	// Drain the pipe in a goroutine so large outputs can't deadlock on the buffer.
	pipeR, pipeW, err := os.Pipe()
	require.NoError(t, err)
	origStderr := os.Stderr
	os.Stderr = pipeW

	var captured bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&captured, pipeR)
		close(done)
	}()

	runErr := uploadToCRLSupport(context.Background(), zipPath)

	_ = pipeW.Close()
	<-done
	_ = pipeR.Close()
	os.Stderr = origStderr

	out := captured.String()
	if runErr != nil {
		out += fmt.Sprintf("ERROR: %v\n", runErr)
	}
	out += fmt.Sprintf(
		"[counters] create_session=%d reupload=%d resumable_session=%d complete=%d blob_probe=%d blob_put=%d\n",
		ctr.createSession.Load(), ctr.reupload.Load(), ctr.resumableSession.Load(),
		ctr.complete.Load(), ctr.blobProbe.Load(), ctr.blobPut.Load(),
	)
	return normalizeScenarioOutput(out)
}

func jsonResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		Status:     http.StatusText(status),
		StatusCode: status,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func buildSessionJSONResponse(req *http.Request, resp *endpointResponse) *http.Response {
	if resp.Body != "" {
		return jsonResponse(req, resp.Status, resp.Body)
	}
	body := ""
	if resp.Status >= 200 && resp.Status < 300 {
		body = fmt.Sprintf(`{"session_id":%q,"upload_token":%q}`,
			resp.SessionID, resp.UploadToken)
	}
	return jsonResponse(req, resp.Status, body)
}

func buildResumableSessionResponse(
	req *http.Request, resp *endpointResponse, blobURI string,
) *http.Response {
	if resp.Body != "" {
		return jsonResponse(req, resp.Status, resp.Body)
	}
	body := ""
	if resp.Status >= 200 && resp.Status < 300 {
		bucket := resp.Bucket
		if bucket == "" {
			bucket = "test-bucket"
		}
		body = fmt.Sprintf(
			`{"gcs_session_uri":%q,"bucket":%q,"object_path":%q,"content_type":"application/zip","reused":false}`,
			blobURI, bucket, resp.ObjectPath,
		)
	}
	return jsonResponse(req, resp.Status, body)
}

func buildBlobProbeResponse(req *http.Request, resp *endpointResponse) *http.Response {
	r := jsonResponse(req, resp.Status, resp.Body)
	if resp.Status == http.StatusPermanentRedirect && resp.RangeEnd > 0 {
		r.Header.Set("Range", fmt.Sprintf("bytes=0-%d", resp.RangeEnd))
	}
	return r
}

func buildRawResponse(req *http.Request, resp *endpointResponse) *http.Response {
	return jsonResponse(req, resp.Status, resp.Body)
}

// Mask per-run values (elapsed, throughput, progress) for stable comparison.
var (
	progressLineRE = regexp.MustCompile(`\rUploading\.\.\.[^\n]*\n?`)
	throughputRE   = regexp.MustCompile(`Uploaded \S+ B in \S+ \(\S+ MB/s\)\.`)
)

func normalizeScenarioOutput(s string) string {
	s = progressLineRE.ReplaceAllString(s, "")
	s = throughputRE.ReplaceAllString(s, "Uploaded NN B in NNs (NN MB/s).")
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, " \t")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
