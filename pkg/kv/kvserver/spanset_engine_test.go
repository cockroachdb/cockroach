// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSpanSetEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create an in-memory engine.
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	declaredKey := roachpb.Key("allowed")
	undeclaredKey := roachpb.Key("forbidden")

	fm := func(span spanset.TrickySpan) error {
		fmt.Printf("CHECKING span: %+v\n", span)
		localStoreSpan := roachpb.Span{
			Key: undeclaredKey,
		}

		if spanset.Overlaps(localStoreSpan, span) {
			return errors.Errorf("overlaps with store local keys")
		}

		return nil
	}

	// Wrap the engine with spanSetEngine.
	wrappedEngine := NewSpanSetEngine(engine)
	wrappedEngine.(*spanSetEngine).AddForbiddenMatcher(fm)

	// Create a batch from the wrapped engine.
	batch := wrappedEngine.NewBatch()
	defer batch.Close()

	// Writing to the declared span should succeed.
	err := batch.PutUnversioned(declaredKey, []byte("value"))
	require.NoError(t, err)

	// Writing to an undeclared span should fail.
	err = batch.PutUnversioned(undeclaredKey, []byte("value"))
	require.Error(t, err)

	err = wrappedEngine.Excise(context.TODO(), roachpb.Span{
		Key:    undeclaredKey,
		EndKey: undeclaredKey.Next(),
	})
	require.Error(t, err)
}
