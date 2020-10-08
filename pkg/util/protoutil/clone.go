// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoutil

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

var verbotenKinds = [...]reflect.Kind{
	reflect.Array,
}

type typeKey struct {
	typ      reflect.Type
	verboten reflect.Kind
}

var types struct {
	syncutil.Mutex
	known map[typeKey]reflect.Type
}

func init() {
	types.known = make(map[typeKey]reflect.Type)
}

// RegisterUnclonableType registers a type as not being allowed for cloning.
// This is an added hack on top of the hack to allow clients of this package to
// disallow cloning of certain types which are not recursed into due to how
// oneof is implemented. In particular it may be the case that one of the
// implementations of an interface is unclonable. In this case, due to the type
// (rather than value) traversal, we'd not discover this fact.
//
// See the comment on Clone.
func RegisterUnclonableType(typ reflect.Type, verbotenKind reflect.Kind) {
	types.Lock()
	defer types.Unlock()
	types.known[typeKey{typ: typ, verboten: verbotenKind}] = typ
}

func uncloneable(pb Message) (reflect.Type, bool) {
	for _, verbotenKind := range verbotenKinds {
		if t := typeIsOrContainsVerboten(reflect.TypeOf(pb), verbotenKind); t != nil {
			return t, true
		}
	}
	return nil, false
}

// Clone uses proto.Clone to return a deep copy of pb. It panics if pb
// recursively contains any instances of types which are known to be
// unsupported by proto.Clone.
//
// This function and its associated lint (see build/style_test.go) exist to
// ensure we do not attempt to proto.Clone types which are not supported by
// proto.Clone. This hackery is necessary because proto.Clone gives no direct
// indication that it has incompletely cloned a type; it merely logs to standard
// output (see
// https://github.com/golang/protobuf/blob/89238a3/proto/clone.go#L204).
//
// The concrete case against which this is currently guarding may be resolved
// upstream, see https://github.com/gogo/protobuf/issues/147.
func Clone(pb Message) Message {
	if t, ok := uncloneable(pb); ok {
		panic(errors.AssertionFailedf("attempt to clone %T, which contains uncloneable field of type %s", pb, t))
	}
	return proto.Clone(pb).(Message)
}

func typeIsOrContainsVerboten(t reflect.Type, verboten reflect.Kind) reflect.Type {
	types.Lock()
	defer types.Unlock()

	return typeIsOrContainsVerbotenLocked(t, verboten)
}

func typeIsOrContainsVerbotenLocked(t reflect.Type, verboten reflect.Kind) reflect.Type {
	key := typeKey{t, verboten}
	knownTypeIsOrContainsVerboten, ok := types.known[key]
	if !ok {
		// To prevent infinite recursion on recursive proto types, put a
		// placeholder in here and immediately overwite it after
		// typeIsOrContainsVerbotenImpl returns.
		types.known[key] = nil
		knownTypeIsOrContainsVerboten = typeIsOrContainsVerbotenImpl(t, verboten)
		types.known[key] = knownTypeIsOrContainsVerboten
	}
	return knownTypeIsOrContainsVerboten
}

func typeIsOrContainsVerbotenImpl(t reflect.Type, verboten reflect.Kind) reflect.Type {
	switch t.Kind() {
	case verboten:
		return t

	case reflect.Map:
		if key := typeIsOrContainsVerbotenLocked(t.Key(), verboten); key != nil {
			return key
		}
		if value := typeIsOrContainsVerbotenLocked(t.Elem(), verboten); value != nil {
			return value
		}

	case reflect.Array, reflect.Ptr, reflect.Slice:
		if value := typeIsOrContainsVerbotenLocked(t.Elem(), verboten); value != nil {
			return value
		}

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if field := typeIsOrContainsVerbotenLocked(t.Field(i).Type, verboten); field != nil {
				return field
			}
		}

	case reflect.Chan, reflect.Func:
		// Not strictly correct, but cloning these kinds is not allowed.
		return t

	}

	return nil
}
