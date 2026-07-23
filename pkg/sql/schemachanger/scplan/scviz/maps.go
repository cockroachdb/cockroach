// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package scviz contains helpers for visualizing schema changer concepts.
package scviz

import (
	"bytes"
	"encoding/json"
	"reflect"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

// ToMap converts a struct to a map, field by field. If at any point a protobuf
// message is encountered, it is converted to a map using jsonpb to marshal it
// to json and then marshaling it back to a map. This approach allows zero
// values to be effectively omitted.
func ToMap(v interface{}, emitDefaults bool) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	if msg, ok := v.(protoutil.Message); ok {
		var buf bytes.Buffer
		jsonEncoder := jsonpb.Marshaler{EmitDefaults: emitDefaults}
		if err := jsonEncoder.Marshal(&buf, msg); err != nil {
			return nil, errors.Wrapf(err, "%T %v", v, v)
		}
		var m map[string]interface{}
		if err := json.NewDecoder(&buf).Decode(&m); err != nil {
			return nil, err
		}
		return m, nil
	}
	vv := reflect.ValueOf(v)
	vt := vv.Type()
	switch vt.Kind() {
	case reflect.Struct:
	case reflect.Ptr:
		if vt.Elem().Kind() != reflect.Struct {
			return v, nil
		}
		vv = vv.Elem()
		vt = vt.Elem()
	default:
		return v, nil
	}

	m := make(map[string]interface{}, vt.NumField())
	for i := 0; i < vt.NumField(); i++ {
		vvf := vv.Field(i)
		if !vvf.CanInterface() || vvf.IsZero() {
			continue
		}
		var err error
		if m[vt.Field(i).Name], err = ToMap(vvf.Interface(), emitDefaults); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// WalkMap calls f for every entry in input recursively, outermost first.
func WalkMap(input interface{}, f func(v interface{})) {
	var walk func(interface{})
	walk = func(obj interface{}) {
		f(obj)
		switch objV := obj.(type) {
		case map[string]interface{}:
			for _, v := range objV {
				walk(v)
			}
		case []interface{}:
			for _, v := range objV {
				walk(v)
			}
		}
	}
	walk(input)
}

// RewriteEmbeddedIntoParent is a rewrite function which lifts embedded
// struct fields into their parent map. It is intended to be used as an
// argument to WalkMap.
func RewriteEmbeddedIntoParent(in interface{}) {
	inM, ok := in.(map[string]interface{})
	if !ok {
		return
	}
	for k, v := range inM {
		m, ok := v.(map[string]interface{})
		if !ok || !embeddedRegexp.MatchString(k) || containsAnyKeys(inM, m) {
			continue
		}
		delete(inM, k)
		for mk, mv := range m {
			inM[mk] = mv
		}
	}
}

var embeddedRegexp = regexp.MustCompile("^embedded[A-Z]")

func containsAnyKeys(haystack, needles map[string]interface{}) bool {
	for k := range needles {
		if _, exists := haystack[k]; exists {
			return true
		}
	}
	return false
}
