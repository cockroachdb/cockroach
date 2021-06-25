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

type ctxs struct {
	ctx0, ctx1, ctx2, ctx3 context.Context
	cancel0                func()
	cancel1, cancel2       func(...interface{})
}

// context.Background <- (ctx0, cancel0) <- (ctx1, hook1, cancel1) <- (ctx2, hook2, cancel2) <- ctx3
func mkCtxChain(hook1, hook2 func(...interface{}) interface{}) ctxs {
	ctx0, cancel0 := context.WithCancel(context.Background())
	ctx1, cancel1 := WithExtCancel(ctx0, hook1)
	ctx2, cancel2 := WithExtCancel(ctx1, hook2)
	ctx3 := context.WithValue(ctx2, "dummy", nil)
	return ctxs{
		ctx0: ctx0, ctx1: ctx1, ctx2: ctx2, ctx3: ctx3,
		cancel0: cancel0, cancel1: cancel1, cancel2: cancel2,
	}
}

func mkHook(ret interface{}) func(...interface{}) interface{} {
	return func(...interface{}) interface{} {
		return ret
	}
}

func TestWithExtCancel(t *testing.T) {
	err1 := errors.New("hook1")
	err2 := errors.New("hook2")
	_, _ = err1, err2
	tests := []struct {
		name   string
		do     func(*testing.T, ctxs)
		h1, h2 func(...interface{}) interface{}
	}{
		{name: "basics", do: func(t *testing.T, c ctxs) {
			require.Nil(t, c.ctx1.Err())
			require.Nil(t, c.ctx2.Err())
			require.Nil(t, c.ctx3.Err())
		}},
		{name: "cancel0", do: func(t *testing.T, c ctxs) {
			// Canceling a non-ext parent gives the standard unfriendly error,
			// even from ExtErr.
			c.cancel0()
			require.Equal(t, context.Canceled, c.ctx1.Err())
			require.Equal(t, context.Canceled, c.ctx2.Err())
			require.Equal(t, context.Canceled, c.ctx3.Err())
			require.Equal(t, context.Canceled, ExtErr(c.ctx1))
			require.Equal(t, context.Canceled, ExtErr(c.ctx2))
			require.Equal(t, context.Canceled, ExtErr(c.ctx3))
			require.Equal(t, context.Canceled, ExtCanceled(c.ctx1))
			require.Equal(t, context.Canceled, ExtCanceled(c.ctx2))
			require.Equal(t, context.Canceled, ExtCanceled(c.ctx3))
		}},
		{name: "cancel1", do: func(t *testing.T, c ctxs) {
			// ctx1 is an extended context, so it does nice things.
			c.cancel1()
			require.Equal(t, context.Canceled, c.ctx1.Err())
			require.Equal(t, context.Canceled, c.ctx2.Err())
			require.Equal(t, context.Canceled, c.ctx3.Err())
			require.Equal(t, err1, ExtErr(c.ctx1))
			require.Equal(t, err1, ExtErr(c.ctx2))
			require.Equal(t, err1, ExtErr(c.ctx3)) // vanilla context
			require.Equal(t, err1, ExtCanceled(c.ctx1))
			require.Equal(t, err1, ExtCanceled(c.ctx2))
			require.Equal(t, err1, ExtCanceled(c.ctx3)) // vanilla context
		}},
		{name: "cancel2", do: func(t *testing.T, c ctxs) {
			// ctx2 is an extended context, so it does nice things.
			c.cancel2()
			require.Nil(t, c.ctx1.Err())
			require.Equal(t, context.Canceled, c.ctx2.Err())
			require.Equal(t, context.Canceled, c.ctx3.Err())
			require.Nil(t, ExtErr(c.ctx1))
			require.Equal(t, err2, ExtErr(c.ctx2))
			require.Equal(t, err2, ExtErr(c.ctx3)) // vanilla context
			require.Nil(t, nil, ExtCanceled(c.ctx1))
			require.Equal(t, err2, ExtCanceled(c.ctx2))
			require.Equal(t, err2, ExtCanceled(c.ctx3)) // vanilla context
		}},
		{name: "return-non-error", do: func(t *testing.T, c ctxs) {
			c.cancel2("i", "am", "sailing")
			require.Equal(t, "iamsailing", ExtCanceled(c.ctx2))
			require.Equal(t, context.Canceled, ExtErr(c.ctx2))
		}, h2: func(args ...interface{}) interface{} { return fmt.Sprint(args...) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.h1 == nil {
				tt.h1 = mkHook(err1)
			}
			if tt.h2 == nil {
				tt.h2 = mkHook(err2)
			}

			tt.do(t, mkCtxChain(tt.h1, tt.h2))
		})
	}
}

func TestWithCallerCancel(t *testing.T) {
	ctx, cancel := WithCallerCancel(context.Background())
	cancel()
	err := ExtErr(ctx)
	require.NotNil(t, err)
	_, _, fn, ok := errors.GetOneLineSource(err)
	require.True(t, ok)
	require.Equal(t, "TestWithCallerCancel", fn, "%+v", err)
}
