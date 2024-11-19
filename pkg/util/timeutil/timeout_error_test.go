// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors/errbase"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecode(t *testing.T) {
	ctx := context.Background()
	{
		origErr := &TimeoutError{
			operation: "hello",
			timeout:   3 * time.Minute,
			cause:     fmt.Errorf("woo"),
		}
		enc := errbase.EncodeError(ctx, origErr)
		newErr := errbase.DecodeError(ctx, enc)

		assert.Equal(t, origErr.Error(), newErr.Error())
		assert.Equal(t, origErr, newErr)
	}

	{
		origErr := &TimeoutError{
			operation: "hello",
			timeout:   3 * time.Minute,
			took:      4 * time.Minute,
			cause:     fmt.Errorf("woo"),
		}
		enc := errbase.EncodeError(ctx, origErr)
		newErr := errbase.DecodeError(ctx, enc)

		assert.Equal(t, origErr.Error(), newErr.Error())
		assert.Equal(t, origErr, newErr)
	}
}
