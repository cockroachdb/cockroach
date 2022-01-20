// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/stretchr/testify/require"
)

func TestCancelRequestProtocol(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s1 := uint128.FromInts(123, 1).String()
	s2 := uint128.FromInts(123, 2).String()
	ctx := context.Background()

	for _, proto := range []CancelRequestProtocol{
		SimpleCancelProtocol,
		HMACCancelProtocol,
	} {
		t.Run(string(proto), func(t *testing.T) {
			auth1, err := NewCancelRequestAuthenticator(proto)
			require.NoError(t, err)
			require.NoError(t, auth1.Initialize(s1))
			auth2, err := NewCancelRequestAuthenticator(proto)
			require.NoError(t, err)
			require.NoError(t, auth2.Initialize(s2))

			ckey1 := auth1.GetClientKey()
			ckey1s := ckey1.String()
			client1, err := NewCancelRequestClient(s1, ckey1s)
			require.NoError(t, err)
			ckey2 := auth2.GetClientKey()
			ckey2s := ckey2.String()
			client2, err := NewCancelRequestClient(s2, ckey2s)
			require.NoError(t, err)

			t.Run("good", func(t *testing.T) {
				// Process retries if requested by protocol.
				for i := 0; ; i++ {
					t.Logf("client: %+v", client1)

					s1c, req := client1.MakeRequest()
					require.Equal(t, s1, s1c)
					reqs := req.String()

					t.Logf("cancel request: %q", reqs)

					creq, err := auth1.ParseCancelRequest(reqs)
					require.NoError(t, err)

					t.Logf("authenticator: %+v", auth1)

					ok, shouldRetry, update, err := auth1.Authenticate(ctx, s1c, creq)
					require.NoError(t, err)
					if shouldRetry {
						if i > 10 {
							t.Fatalf("too many iterations: %d", i)
						}
						t.Logf("retry update: %q", update)
						require.NoError(t, client1.UpdateFromServer(update))
						continue
					}
					require.True(t, ok)
					break
				}
			})
			t.Run("bad", func(t *testing.T) {
				// Process retries if requested by protocol.
				for i := 0; ; i++ {
					t.Logf("client: %+v", client2)

					s2c, req := client2.MakeRequest()
					require.Equal(t, s2, s2c)
					reqs := req.String()

					t.Logf("cancel request: %q", reqs)

					// Use the wrong authenticator.
					// Request should still parse.
					creq, err := auth1.ParseCancelRequest(reqs)
					require.NoError(t, err)

					t.Logf("authenticator: %+v", auth1)

					ok, shouldRetry, update, err := auth1.Authenticate(ctx, s2c, creq)
					require.NoError(t, err)
					if shouldRetry {
						if i > 10 {
							t.Fatalf("too many iterations: %d", i)
						}
						t.Logf("retry update: %q", update)
						require.NoError(t, client2.UpdateFromServer(update))
						continue
					}
					// Expect authentication fails.
					require.False(t, ok)
					break
				}
			})
		})
	}
}
