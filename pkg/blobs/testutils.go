// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TestBlobServiceClient can be used as a mock BlobClient
// in tests that use nodelocal storage.
func TestBlobServiceClient(externalIODir string) BlobClientFactory {
	return func(ctx context.Context, dialing roachpb.NodeID) (BlobClient, error) {
		return NewLocalClient(externalIODir)
	}
}

// TestEmptyBlobClientFactory can be used as a mock BlobClient
// in tests that create ExternalStorage but do not use
// nodelocal storage.
var TestEmptyBlobClientFactory = func(
	ctx context.Context, dialing roachpb.NodeID,
) (BlobClient, error) {
	return nil, nil
}
