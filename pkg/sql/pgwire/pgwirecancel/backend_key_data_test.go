// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwirecancel

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestMakeBackendKeyData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	b1 := MakeBackendKeyData(rng, base.SQLInstanceID(1))
	b2 := MakeBackendKeyData(rng, base.SQLInstanceID(1))
	require.NotEqual(t, b1, b2)
}

func TestGetSQLInstanceID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	t.Run("small id", func(t *testing.T) {
		for i := 0; i < 1<<11; i++ {
			b := MakeBackendKeyData(rng, base.SQLInstanceID(i))
			require.Equal(t, base.SQLInstanceID(i), b.GetSQLInstanceID())
		}
	})

	for i := 1 << 11; i < math.MaxInt32; i = (i * 2) + 1 {
		t.Run(fmt.Sprintf("large id %d", i), func(t *testing.T) {
			b := MakeBackendKeyData(rng, base.SQLInstanceID(i))
			require.Equal(t, base.SQLInstanceID(i), b.GetSQLInstanceID())
		})
	}
}
