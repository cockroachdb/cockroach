// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestRewriteKeyToTenantPrefix(t *testing.T) {
	for _, tc := range []struct {
		oldTenant, newTenant uint64
		suffix               string
	}{
		{1, 1, "abc"},
		{1, 2, "acc"},
		{2, 1, "abc"},
		{1, 1 << 15, "abc"},
		{1 << 15, 1, "abc"},
		{2, 1 << 15, "abc"},
		{1 << 15, 2, "abc"},
		{1 << 15, 1 << 15, "abc"},
		{1 << 13, 1 << 15, "abc"},
		{1 << 15, 1 << 13, "abc"},
	} {
		t.Run(fmt.Sprintf("%d to %d %s", tc.oldTenant, tc.newTenant, tc.suffix), func(t *testing.T) {
			old := append(MakeSQLCodec(roachpb.MustMakeTenantID(tc.oldTenant)).TablePrefix(5), tc.suffix...)
			new := MakeTenantPrefix(roachpb.MustMakeTenantID(tc.newTenant))
			got, err := RewriteKeyToTenantPrefix(old, new)
			require.NoError(t, err)

			expect := append(MakeSQLCodec(roachpb.MustMakeTenantID(tc.newTenant)).TablePrefix(5), tc.suffix...)
			require.Equal(t, expect, got)
		})
	}
}
