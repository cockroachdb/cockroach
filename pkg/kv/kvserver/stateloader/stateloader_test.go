// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stateloader

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUninitializedReplicaState(t *testing.T) {
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	desc := roachpb.RangeDescriptor{RangeID: 123}
	exp, err := Make(desc.RangeID).Load(context.Background(), eng, &desc)
	require.NoError(t, err)
	act := UninitializedReplicaState(desc.RangeID)
	require.Equal(t, exp, act)
}
