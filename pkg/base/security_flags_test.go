// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllSecurityOverridesHaveName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for i := SecurityOverrides(1); i < DisableAll; i = i * 2 {
		// Check that all settings have a name.
		name := i.String()
		require.NotEmpty(t, name)

		// Check that setting a new override set from that name
		// yields the same set of flags.
		copy := i
		copy.Validate()
		var j SecurityOverrides
		require.NoError(t, j.Set(name))
		assert.Equal(t, j, copy)
	}
}

func TestSecurityOverridesValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		in, out string
	}{
		{"disable-all", "disable-tls,disable-rpc-tls,disable-rpc-authn,disable-sql-authn,disable-sql-tls,disable-sql-require-tls,disable-sql-set-credentials,disable-http-tls,disable-http-authn,disable-remote-certs-retrieval,disable-cluster-name-verification"},
		{"disable-tls", "disable-tls,disable-rpc-tls,disable-rpc-authn,disable-sql-tls,disable-sql-require-tls,disable-http-tls,disable-remote-certs-retrieval"},
		{"disable-rpc-tls", "disable-rpc-tls,disable-rpc-authn,disable-remote-certs-retrieval"},
		{"disable-rpc-authn", "disable-rpc-tls,disable-rpc-authn,disable-remote-certs-retrieval"},
		{"disable-sql-tls", "disable-sql-tls,disable-sql-require-tls"},
	}

	for _, tc := range testCases {
		var fl SecurityOverrides
		require.NoError(t, fl.Set(tc.in))
		assert.Equal(t, fl.String(), tc.out)
	}
}
