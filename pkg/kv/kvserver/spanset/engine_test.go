// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSpanSetEngine tests that the spanSetEngine correctly enforces forbidden
// spans when performing engine operations.
func TestSpanSetEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create an in-memory engine.
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	allowedKey := roachpb.Key("allowed")
	forbiddenKey := roachpb.Key("forbidden")
	fm := func(span TrickySpan) error {
		if Overlaps(roachpb.Span{
			Key: forbiddenKey,
		}, span) {
			return errors.Errorf("forbidden access: %s", span)
		}
		return nil
	}

	// Wrap the engine with spanSetEngine.
	wrappedEngine := NewEngine(engine, fm)

	// Create a batch from the wrapped engine.
	batch := wrappedEngine.NewBatch()
	defer batch.Close()

	// Writing to the declared span should succeed.
	err := batch.PutUnversioned(allowedKey, []byte("value"))
	require.NoError(t, err)

	// Writing to an undeclared span should fail.
	err = batch.PutUnversioned(forbiddenKey, []byte("value"))
	require.Error(t, err)

	// Run a direct engine call to Excise on a non-declared span; it should fail.
	err = wrappedEngine.Excise(context.Background(), roachpb.Span{
		Key:    forbiddenKey,
		EndKey: forbiddenKey.Next(),
	})
	require.Error(t, err)

	// Run a direct engine call to Excise on a declared span; it should succeed.
	err = wrappedEngine.Excise(context.Background(), roachpb.Span{
		Key:    allowedKey,
		EndKey: allowedKey.Next(),
	})
	require.NoError(t, err)
}
