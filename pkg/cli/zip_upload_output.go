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
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// uploadZipOutput implements the zipOutput interface by uploading
// artifacts to a remote upload server instead of writing to a local
// zip file.
type uploadZipOutput struct {
	mu     syncutil.Mutex
	client *uploadServerClient
	nodeID int32
	ctx    context.Context

	// artifacts counts the number of artifacts successfully uploaded.
	artifacts int

	// pendingBuf and pendingName are used by the createLocked/Unlock
	// pattern. createLocked sets up a buffer that the caller writes
	// into while holding the lock. On Unlock, if pendingBuf is
	// non-nil, its contents are uploaded to the server.
	pendingBuf  *bytes.Buffer
	pendingName string
}

var _ zipOutput = (*uploadZipOutput)(nil)

// newUploadZipOutput creates a new uploadZipOutput for the given node.
func newUploadZipOutput(
	ctx context.Context, client *uploadServerClient, nodeID int32,
) *uploadZipOutput {
	return &uploadZipOutput{
		client: client,
		nodeID: nodeID,
		ctx:    ctx,
	}
}

// Lock acquires the mutex. This is part of the zipOutput interface
// contract so that callers can hold the lock across
// createLocked/Write/Unlock sequences.
func (u *uploadZipOutput) Lock() {
	u.mu.Lock()
}

// Unlock releases the mutex. If a pending buffer was set up by
// createLocked, its contents are uploaded before releasing the lock.
func (u *uploadZipOutput) Unlock() {
	if u.pendingBuf != nil {
		buf := u.pendingBuf
		name := u.pendingName
		u.pendingBuf = nil
		u.pendingName = ""
		// Upload outside the critical section is not possible here
		// because the caller expects the upload to be done when Unlock
		// returns. We upload while holding the lock.
		if err := u.uploadBytes(name, buf.Bytes()); err != nil {
			// Log the error but don't panic; the caller has no way to
			// receive this error from Unlock. The data is best-effort.
			fmt.Printf("WARNING: failed to upload %s: %v\n", name, err)
		}
	}
	u.mu.Unlock()
}

// AssertHeld asserts that the mutex is held.
func (u *uploadZipOutput) AssertHeld() {
	u.mu.AssertHeld()
}

// createLocked prepares a buffer for writing. The caller must hold
// the lock. The returned io.Writer is backed by an in-memory buffer.
// The buffer contents are uploaded when Unlock is called.
func (u *uploadZipOutput) createLocked(name string, _ time.Time) (io.Writer, error) {
	u.mu.AssertHeld()
	u.pendingBuf = &bytes.Buffer{}
	u.pendingName = name
	return u.pendingBuf, nil
}

// createRaw uploads raw bytes as an artifact.
func (u *uploadZipOutput) createRaw(s *zipReporter, name string, b []byte) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	s.progress("uploading: %s", name)
	if err := u.uploadBytes(name, b); err != nil {
		return s.fail(err)
	}
	s.done()
	return nil
}

// createJSON marshals a value to JSON and uploads it.
func (u *uploadZipOutput) createJSON(s *zipReporter, name string, m interface{}) error {
	if !strings.HasSuffix(name, ".json") {
		return s.fail(errors.Errorf("%s does not have .json suffix", name))
	}
	s.progress("uploading JSON: %s", name)

	u.mu.Lock()
	defer u.mu.Unlock()

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		return s.fail(err)
	}
	if err := u.uploadBytes(name, buf.Bytes()); err != nil {
		return s.fail(err)
	}
	s.done()
	return nil
}

// createError uploads an error description as a .err.txt file.
func (u *uploadZipOutput) createError(s *zipReporter, name string, e error) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	s.shout("last request failed: %v", e)
	out := name + ".err.txt"
	s.progress("uploading error: %s", out)
	errText := fmt.Sprintf("%+v\n", e)
	if err := u.uploadBytes(out, []byte(errText)); err != nil {
		return s.fail(err)
	}
	s.done()
	return nil
}

// createJSONOrError calls either createError or createJSON depending
// on whether the error argument is nil.
func (u *uploadZipOutput) createJSONOrError(
	s *zipReporter, name string, m interface{}, e error,
) error {
	if e != nil {
		return u.createError(s, name, e)
	}
	return u.createJSON(s, name, m)
}

// createRawOrError calls either createError or createRaw depending
// on whether the error argument is nil.
func (u *uploadZipOutput) createRawOrError(s *zipReporter, name string, b []byte, e error) error {
	if filepath.Ext(name) == "" {
		return errors.Errorf("%s has no extension", name)
	}
	if e != nil {
		return u.createError(s, name, e)
	}
	return u.createRaw(s, name, b)
}

// close is a no-op for the upload output since there is no local file
// to finalize.
func (u *uploadZipOutput) close() error {
	return nil
}

// artifactsUploaded returns the count of artifacts uploaded so far.
func (u *uploadZipOutput) artifactsUploaded() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.artifacts
}

// uploadBytes is the common upload path. Must be called with mu held.
func (u *uploadZipOutput) uploadBytes(name string, data []byte) error {
	u.mu.AssertHeld()
	artType := inferArtifactType(name)
	contentType := inferContentType(name)
	idempotencyKey := uuid.MakeV4().String()

	err := u.client.UploadArtifact(
		u.ctx, name, u.nodeID, artType, contentType, idempotencyKey, data,
	)
	if err != nil {
		return err
	}
	u.artifacts++
	return nil
}

// inferArtifactType maps file paths to artifact types based on
// extension and path patterns.
func inferArtifactType(name string) artifactType {
	ext := filepath.Ext(name)
	switch ext {
	case ".pprof":
		return artifactTypeProfile
	}
	base := filepath.Base(name)
	switch {
	case strings.Contains(name, "/logs/"):
		return artifactTypeLog
	case base == "stacks.txt" || base == "stacks_with_labels.txt":
		return artifactTypeStack
	case base == "lsm.txt":
		return artifactTypeEngineStats
	case ext == ".json":
		return artifactTypeMetadata
	case ext == ".txt" || ext == ".csv" || ext == ".tsv":
		return artifactTypeTable
	case ext == ".zip":
		return artifactTypeTrace
	case ext == ".sh":
		return artifactTypeMetadata
	default:
		return artifactTypeMetadata
	}
}

// inferContentType returns a Content-Type header value based on file
// extension.
func inferContentType(name string) string {
	ext := filepath.Ext(name)
	switch ext {
	case ".json":
		return "application/json"
	case ".pprof":
		return "application/octet-stream"
	case ".zip":
		return "application/zip"
	case ".txt", ".sh":
		return "text/plain"
	case ".csv":
		return "text/csv"
	case ".tsv":
		return "text/tab-separated-values"
	default:
		return "application/octet-stream"
	}
}
