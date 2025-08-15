package scerrors_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errbase"
	"github.com/stretchr/testify/require"
)

// TestEncodeSchemaChangeUserError tests that the error is encoded and decoded
// correctly if sent over the network.
func TestEncodeSchemaChangeUserError(t *testing.T) {
	ctx := context.Background()
	base := scerrors.SchemaChangerUserError(errors.New("boom"))
	err := errbase.EncodeError(ctx, base)
	decodeError := errbase.DecodeError(ctx, err)
	require.True(t, scerrors.HasSchemaChangerUserError(decodeError))
}
