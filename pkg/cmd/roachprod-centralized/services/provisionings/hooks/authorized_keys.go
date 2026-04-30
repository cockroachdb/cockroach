// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import "context"

// AuthorizedKeysProvider fetches SSH public keys for all engineers.
// The returned bytes are in authorized_keys format (one key per line).
type AuthorizedKeysProvider interface {
	GetAuthorizedKeys(ctx context.Context) ([]byte, error)
}
