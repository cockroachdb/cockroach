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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestBCryptCostToSCRAMIterCountTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test mainly checks that the conversion table properly starts
	// at bcrypt.MinCost, and that no lines are accidentally added or removed.
	require.Equal(t, bcrypt.MaxCost+1, len(bcryptCostToSCRAMIterCount))
	for i := 0; i < bcrypt.MinCost; i++ {
		require.Equal(t, int64(i), bcryptCostToSCRAMIterCount[i])
	}
	require.Equal(t, int64(4096), bcryptCostToSCRAMIterCount[bcrypt.MinCost])
	require.Equal(t, int64(119680), bcryptCostToSCRAMIterCount[10])
}

func TestBCryptToSCRAMConversion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := cluster.MakeTestingClusterSettings()

	const cleartext = "hello"

	ourDefault := int(BcryptCost.Get(&s.SV))

	// Don't iterate all the way to 31: the larger values
	// incur incredibly large hashing times.
	for i := bcrypt.MinCost; i < ourDefault+3; i++ {
		t.Run(fmt.Sprintf("bcrypt=%d", i), func(t *testing.T) {
			bcryptRaw, err := bcrypt.GenerateFromPassword(appendEmptySha256(cleartext), i)
			require.NoError(t, err)
			bh := LoadPasswordHash(ctx, bcryptRaw)
			require.Equal(t, HashBCrypt, bh.Method())

			// Check conversion succeeds.
			converted, prevHash, newHashBytes, hashMethod, err := MaybeUpgradePasswordHash(ctx, &s.SV, cleartext, bh)
			require.NoError(t, err)
			require.True(t, converted)
			require.Equal(t, "SCRAM-SHA-256", string(newHashBytes)[:13])
			require.Equal(t, HashSCRAMSHA256.String(), hashMethod)
			require.Equal(t, bcryptRaw, prevHash)

			newHash := LoadPasswordHash(ctx, newHashBytes)
			sh := newHash.(*ScramHash)
			creds := sh.StoredCredentials()
			require.Equal(t, bcryptCostToSCRAMIterCount[i], int64(creds.Iters))

			// Check that converted hash can't be converted further.
			ec, _, _, _, err := MaybeUpgradePasswordHash(ctx, &s.SV, cleartext, newHash)
			require.NoError(t, err)
			require.False(t, ec)
		})
	}
}
