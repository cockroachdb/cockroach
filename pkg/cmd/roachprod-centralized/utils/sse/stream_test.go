// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sse

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestStream_SendsLogEvents(t *testing.T) {
	pr, pw := io.Pipe()

	r := gin.New()
	r.GET("/test", func(c *gin.Context) {
		Stream(c, pr, nil)
	})
	srv := httptest.NewServer(r)
	defer srv.Close()

	go func() {
		_, _ = fmt.Fprintln(pw, `{"msg":"hello"}`)
		_, _ = fmt.Fprintln(pw, `{"msg":"world"}`)
		_ = pw.Close()
	}()

	resp, err := http.Get(srv.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, `{"msg":"hello"}`)
	assert.Contains(t, bodyStr, `{"msg":"world"}`)
	assert.Contains(t, bodyStr, "event:done")
}

func TestStream_SendsErrorOnPipeError(t *testing.T) {
	pr, pw := io.Pipe()

	r := gin.New()
	r.GET("/test", func(c *gin.Context) {
		Stream(c, pr, nil)
	})
	srv := httptest.NewServer(r)
	defer srv.Close()

	go func() {
		_, _ = fmt.Fprintln(pw, "line1")
		_ = pw.CloseWithError(fmt.Errorf("read failure"))
	}()

	resp, err := http.Get(srv.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "line1")
	assert.Contains(t, bodyStr, "event:error")
	// Raw internal error must not be exposed to the client.
	assert.NotContains(t, bodyStr, "read failure")
	assert.Contains(t, bodyStr, "log stream error")
}

func TestStream_LongLine(t *testing.T) {
	// Verify lines longer than the default 64 KiB scanner limit work
	// after the buffer increase (M2 fix).
	longLine := strings.Repeat("x", 128*1024) // 128 KB

	pr, pw := io.Pipe()

	r := gin.New()
	r.GET("/test", func(c *gin.Context) {
		Stream(c, pr, nil)
	})
	srv := httptest.NewServer(r)
	defer srv.Close()

	go func() {
		_, _ = fmt.Fprintln(pw, longLine)
		_ = pw.Close()
	}()

	resp, err := http.Get(srv.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, longLine)
	assert.Contains(t, bodyStr, "event:done")
	assert.NotContains(t, bodyStr, "event:error")
}

func TestStream_EmptyReader(t *testing.T) {
	pr, pw := io.Pipe()

	r := gin.New()
	r.GET("/test", func(c *gin.Context) {
		Stream(c, pr, nil)
	})
	srv := httptest.NewServer(r)
	defer srv.Close()

	go func() { _ = pw.Close() }()

	resp, err := http.Get(srv.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	// No log events, just a done event.
	assert.NotContains(t, bodyStr, "event:log")
	assert.Contains(t, bodyStr, "event:done")
}

func TestStream_SetsHeaders(t *testing.T) {
	pr, pw := io.Pipe()

	r := gin.New()
	r.GET("/test", func(c *gin.Context) {
		Stream(c, pr, nil)
	})
	srv := httptest.NewServer(r)
	defer srv.Close()

	go func() { _ = pw.Close() }()

	resp, err := http.Get(srv.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
}
