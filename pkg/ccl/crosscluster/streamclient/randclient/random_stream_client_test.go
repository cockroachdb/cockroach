// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randclient

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
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

	streamAddresses := []string{
		"randomgen://test0/",
		"<invalid-url-test1>",
		"randomgen://test2/",
		"invalidScheme://test3",
		"randomgen://test4/",
		"randomgen://test5/",
		"randomgen://test6/",
	}
	addressDialCount := map[string]int{}
	for _, addr := range streamAddresses {
		addressDialCount[addr] = 0
	}

	// Track dials and error for all but test3 and test4
	client.RegisterDialInterception(func(streamURL *url.URL) error {
		addr := streamURL.String()
		addressDialCount[addr]++
		if addr != streamAddresses[3] && addr != streamAddresses[4] {
			return errors.Errorf("injected dial error")
		}
		return nil
	})

	activeClient, err := streamclient.GetFirstActiveClient(context.Background(), streamAddresses, nil)
	require.NoError(t, err)

	// Should've dialed the valid schemes up to the 5th one where it should've
	// succeeded
	require.Equal(t, 1, addressDialCount[streamAddresses[0]])
	require.Equal(t, 0, addressDialCount[streamAddresses[1]])
	require.Equal(t, 1, addressDialCount[streamAddresses[2]])
	require.Equal(t, 0, addressDialCount[streamAddresses[3]])
	require.Equal(t, 1, addressDialCount[streamAddresses[4]])
	require.Equal(t, 0, addressDialCount[streamAddresses[5]])
	require.Equal(t, 0, addressDialCount[streamAddresses[6]])

	// The 5th should've succeded as it was a valid scheme and succeeded Dial
	require.Equal(t, activeClient.(streamclient.RandomClient).URL(), streamAddresses[4])
}
