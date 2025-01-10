// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randclient

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestGetFirstActiveClient tests GetFirstActiveClient. It exists in this package
// to avoid circular dependencies.
func TestGetFirstActiveClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	client := streamclient.GetRandomStreamClientSingletonForTesting()
	defer func() {
		require.NoError(t, client.Close(context.Background()))
	}()

	streamAddresses := []streamclient.ClusterUri{
		streamclient.MakeTestClusterUri(url.URL{Scheme: "randomgen", Host: "test0"}),
		streamclient.MakeTestClusterUri(url.URL{Scheme: "", Host: "<invalid-url-test1>"}),
		streamclient.MakeTestClusterUri(url.URL{Scheme: "randomgen", Host: "test2"}),
		streamclient.MakeTestClusterUri(url.URL{Scheme: "invalidScheme", Host: "test3"}),
		streamclient.MakeTestClusterUri(url.URL{Scheme: "randomgen", Host: "test4"}),
		streamclient.MakeTestClusterUri(url.URL{Scheme: "randomgen", Host: "test5"}),
		streamclient.MakeTestClusterUri(url.URL{Scheme: "randomgen", Host: "test6"}),
	}
	uriDialCount := map[url.URL]int{}
	for _, addr := range streamAddresses {
		uriDialCount[addr.URL()] = 0
	}

	// Track dials and error for all but test3 and test4
	client.RegisterDialInterception(func(streamURL url.URL) error {
		uriDialCount[streamURL]++
		if streamURL != streamAddresses[3].URL() && streamURL != streamAddresses[4].URL() {
			return errors.Errorf("injected dial error")
		}
		return nil
	})

	activeClient, err := streamclient.GetFirstActiveClient(context.Background(), streamAddresses, nil)
	require.NoError(t, err)

	// Should've dialed the valid schemes up to the 5th one where it should've
	// succeeded
	require.Equal(t, 1, uriDialCount[streamAddresses[0].URL()])
	require.Equal(t, 0, uriDialCount[streamAddresses[1].URL()])
	require.Equal(t, 1, uriDialCount[streamAddresses[2].URL()])
	require.Equal(t, 0, uriDialCount[streamAddresses[3].URL()])
	require.Equal(t, 1, uriDialCount[streamAddresses[4].URL()])
	require.Equal(t, 0, uriDialCount[streamAddresses[5].URL()])
	require.Equal(t, 0, uriDialCount[streamAddresses[6].URL()])

	// The 5th should've succeded as it was a valid scheme and succeeded Dial
	require.Equal(t, activeClient.(streamclient.RandomClient).URL(), streamAddresses[4])
}
