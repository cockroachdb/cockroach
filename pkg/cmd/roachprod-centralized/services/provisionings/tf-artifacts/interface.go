// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package artifacts

import (
	"context"
	"io"
)

// Store persists provisioning artifacts outside the repository.
type Store interface {
	Put(ctx context.Context, objectKey string, r io.Reader, contentType string) (ref string, err error)
	NewReader(ctx context.Context, ref string) (io.ReadCloser, error)
	Delete(ctx context.Context, ref string) error
}
