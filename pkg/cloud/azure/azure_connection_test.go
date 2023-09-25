// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/stretchr/testify/require"
)

func TestAzureStorageConnection(t *testing.T) {
	require.Equal(t, connectionpb.ConnectionProvider_azure_storage, externalconn.ProviderForURI("azure://test"))
	require.Equal(t, connectionpb.ConnectionProvider_azure_storage, externalconn.ProviderForURI("azure-storage://test"))
	require.Equal(t, connectionpb.ConnectionProvider_azure_storage, externalconn.ProviderForURI("azure-blob://test"))
}
