// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
