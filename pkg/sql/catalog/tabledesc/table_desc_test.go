// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/stretchr/testify/require"
)

func TestMaybeIncrementVersion(t *testing.T) {
	// Created descriptors should not have their version incremented.
	t.Run("created does not get incremented", func(t *testing.T) {
		{
			mut := NewBuilder(&descpb.TableDescriptor{
				ID:      1,
				Version: 1,
			}).BuildCreatedMutableTable()
			mut.MaybeIncrementVersion()
			require.Equal(t, descpb.DescriptorVersion(1), mut.GetVersion())
		}
		{
			mut := NewBuilder(&descpb.TableDescriptor{
				ID:      1,
				Version: 42,
			}).BuildCreatedMutableTable()
			mut.MaybeIncrementVersion()
			require.Equal(t, descpb.DescriptorVersion(42), mut.GetVersion())
		}
	})
	t.Run("existed gets incremented once", func(t *testing.T) {
		mut := NewBuilder(&descpb.TableDescriptor{
			ID:      1,
			Version: 1,
		}).BuildExistingMutableTable()
		require.Equal(t, descpb.DescriptorVersion(1), mut.GetVersion())
		mut.MaybeIncrementVersion()
		require.Equal(t, descpb.DescriptorVersion(2), mut.GetVersion())
		mut.MaybeIncrementVersion()
		require.Equal(t, descpb.DescriptorVersion(2), mut.GetVersion())
	})
}
