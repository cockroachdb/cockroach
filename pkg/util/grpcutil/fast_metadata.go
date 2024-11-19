// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcutil

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
)

// FastFromIncomingContext is a specialization of
// metadata.FromIncomingContext() which extracts the metadata.MD from
// the context, if any, by reference. Main differences:
//
//   - This variant does not guarantee that all the MD keys are
//     lowercase. This happens to be true when the MD is populated by
//     gRPC itself on an incoming RPC call, but it may not be true for
//     MD populated elsewhere.
//   - The caller promises to not modify the returned MD -- the gRPC
//     APIs assume that the map in the context remains constant.
func FastFromIncomingContext(ctx context.Context) (metadata.MD, bool) {
	md, ok := ctx.Value(grpcIncomingKeyObj).(metadata.MD)
	return md, ok
}

// ClearIncomingContext "removes" the gRPC incoming metadata
// if there's any already. No-op if there was no metadata
// to start with.
func ClearIncomingContext(ctx context.Context) context.Context {
	if ctx.Value(grpcIncomingKeyObj) != nil {
		return metadata.NewIncomingContext(ctx, nil)
	}
	return ctx
}

// ClearIncomingContextExcept removes the gRPC incoming metadata if
// there is any, except for metadata items identified by the given
// keys.
func ClearIncomingContextExcept(ctx context.Context, keys ...string) context.Context {
	oldMD, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ClearIncomingContext(ctx)
	}
	newMD := metadata.New(nil)
	for _, k := range keys {
		newMD[k] = oldMD.Get(k)
	}
	return metadata.NewIncomingContext(ctx, newMD)
}

// FastFirstValueFromIncomingContext is a specialization of
// metadata.ValueFromIncomingContext() which extracts the first string
// from the given metadata key, if it exists. No extra objects are
// allocated. The key is assumed to contain only ASCII characters.
func FastFirstValueFromIncomingContext(ctx context.Context, key string) (string, bool) {
	md, ok := ctx.Value(grpcIncomingKeyObj).(metadata.MD)
	if !ok {
		return "", false
	}
	if v, ok := md[key]; ok {
		if len(v) > 0 {
			return v[0], true
		}
		return "", false
	}
	for k, v := range md {
		// The letter casing may not have been set properly when MD was
		// attached to the context. So we need to normalize it here.
		//
		// We add len(k) == len(key) to avoid the overhead of
		// strings.ToLower when the keys of different length, because then
		// they are guaranteed to not match anyway. This is the
		// optimization that requires the key to be all ASCII, as
		// generally ToLower() on non-ascii unicode can change the length
		// of the string.
		if len(k) == len(key) && strings.ToLower(k) == key {
			if len(v) > 0 {
				return v[0], true
			}
			return "", false
		}
	}
	return "", false
}

// grpcIncomingKeyObj is a copy of a value with the Go type
// `metadata.incomingKey{}` (from the grpc metadata package). We
// cannot construct an object of that type directly, but we can
// "steal" it by forcing the metadata package to give it to us:
// `metadata.FromIncomingContext` gives an instance of this object as
// parameter to the `Value` method of the context you give it as
// argument. We use a custom implementation of that to "steal" the
// argument of type `incomingKey{}` given to us that way.
var grpcIncomingKeyObj = func() interface{} {
	var f fakeContext
	_, _ = metadata.FromIncomingContext(&f)
	if f.recordedKey == nil {
		panic("ValueFromIncomingContext did not request a key")
	}
	return f.recordedKey
}()

type fakeContext struct {
	recordedKey interface{}
}

var _ context.Context = (*fakeContext)(nil)

// Value implements the context.Context interface and is our helper
// that "steals" an instance of the private type `incomingKey` in the
// grpc metadata package.
func (f *fakeContext) Value(keyObj interface{}) interface{} {
	f.recordedKey = keyObj
	return nil
}

func (*fakeContext) Deadline() (time.Time, bool) { panic("unused") }
func (*fakeContext) Done() <-chan struct{}       { panic("unused") }
func (*fakeContext) Err() error                  { panic("unused") }
