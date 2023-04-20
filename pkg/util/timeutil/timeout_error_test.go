// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
