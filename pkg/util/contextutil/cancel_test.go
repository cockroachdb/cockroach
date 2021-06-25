// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contextutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func ExampleWithErrCancel() {
	ctx1, cancel1 := WithErrCancel(context.Background())
	ctx2, cancel2 := WithCancel(ctx1)
	ctx3, cancel3 := WithErrCancel(ctx2)

	cancel3(errors.New("boom"))
	fmt.Println("ctx3 canceled:")
	fmt.Println(Err(ctx1))
	fmt.Println(Err(ctx2))
	fmt.Println(Err(ctx3))
	cancel2()
	fmt.Println("ctx2 also canceled:")
	fmt.Println(Err(ctx1))
	fmt.Println(Err(ctx2))
	fmt.Println(Err(ctx3))
	cancel1(errors.New("explody"))
	fmt.Println("ctx1 also canceled:")
	fmt.Println(Err(ctx1))
	fmt.Println(Err(ctx2))
	fmt.Println(Err(ctx3))

	// Output:
	//
	// ctx3 canceled:
	// <nil>
	// <nil>
	// boom
	// ctx2 also canceled:
	// <nil>
	// context canceled
	// boom
	// ctx1 also canceled:
	// explody
	// explody
	// explody
}

type ctxs struct {
	ctx0, ctx1, ctx2, ctx3 context.Context
	cancel0                func()
	cancel1, cancel2       func(error)
}

type testKey struct{}

// context.Background <- (ctx0, cancel0) <- (ctx1, cancel1) <- (ctx2, cancel2) <- ctx3
func mkCtxChain() ctxs {
	ctx0, cancel0 := context.WithCancel(context.Background())
	ctx1, cancel1 := WithErrCancel(ctx0)
	ctx2, cancel2 := WithErrCancel(ctx1)
	ctx3 := context.WithValue(ctx2, testKey{}, nil)
	return ctxs{
		ctx0: ctx0, ctx1: ctx1, ctx2: ctx2, ctx3: ctx3,
		cancel0: cancel0, cancel1: cancel1, cancel2: cancel2,
	}
}

func TestWithErrCancel(t *testing.T) {
	err1 := errors.New("err1")
	err2 := errors.New("err2")
	tests := []struct {
		name string
		do   func(*testing.T, ctxs)
	}{
		{name: "basics", do: func(t *testing.T, c ctxs) {
			require.Nil(t, c.ctx1.Err())
			require.Nil(t, c.ctx2.Err())
			require.Nil(t, c.ctx3.Err())
			require.Nil(t, Err(c.ctx1))
			require.Nil(t, Err(c.ctx2))
			require.Nil(t, Err(c.ctx3))
		}},
		{name: "cancel0", do: func(t *testing.T, c ctxs) {
			// Canceling a regular parent gives the standard unfriendly error,
			// even from Err.
			c.cancel0()
			require.Equal(t, context.Canceled, c.ctx1.Err())
			require.Equal(t, context.Canceled, c.ctx2.Err())
			require.Equal(t, context.Canceled, c.ctx3.Err())
			require.Equal(t, context.Canceled, Err(c.ctx1))
			require.Equal(t, context.Canceled, Err(c.ctx2))
			require.Equal(t, context.Canceled, Err(c.ctx3))
		}},
		{name: "cancel1", do: func(t *testing.T, c ctxs) {
			// ctx1 is an extended context, so it does nice things.
			c.cancel1(err1)
			require.Equal(t, context.Canceled, c.ctx1.Err())
			require.Equal(t, context.Canceled, c.ctx2.Err())
			require.Equal(t, context.Canceled, c.ctx3.Err())
			require.True(t, errors.Is(Err(c.ctx1), err1))
			require.True(t, errors.Is(Err(c.ctx2), err1))
			require.True(t, errors.Is(Err(c.ctx3), err1)) // vanilla context
		}},
		{name: "cancel2", do: func(t *testing.T, c ctxs) {
			// ctx2 is an extended context, so it does nice things.
			c.cancel2(err2)
			require.Nil(t, c.ctx1.Err())
			require.Equal(t, context.Canceled, c.ctx2.Err())
			require.Equal(t, context.Canceled, c.ctx3.Err())
			require.Nil(t, Err(c.ctx1))
			require.True(t, errors.Is(Err(c.ctx2), err2))
			require.True(t, errors.Is(Err(c.ctx3), err2)) // vanilla context
		}},
		{name: "cancel123", do: func(t *testing.T, c ctxs) {
			// When multiple rich contexts are canceled, we get the topmost
			// nice error back.
			c.cancel0()
			c.cancel1(err1)
			c.cancel2(err2)
			require.Equal(t, context.Canceled, c.ctx1.Err())
			require.Equal(t, context.Canceled, c.ctx2.Err())
			require.Equal(t, context.Canceled, c.ctx3.Err())
			require.True(t, errors.Is(Err(c.ctx1), err1))
			require.True(t, errors.Is(Err(c.ctx2), err1))
			require.True(t, errors.Is(Err(c.ctx3), err1)) // vanilla context
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.do(t, mkCtxChain())
		})
	}
}

func TestWithErrCancelStack(t *testing.T) {
	ctx, cancel := WithErrCancel(context.Background())
	cancel(errors.New("boom"))
	err := Err(ctx)
	require.NotNil(t, err)
	_, _, fn, ok := errors.GetOneLineSource(err)
	require.True(t, ok)
	require.Equal(t, "TestWithErrCancelStack", fn, "%+v", err)
}
