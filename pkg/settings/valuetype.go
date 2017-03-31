// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package settings

import (
	"strconv"

	"github.com/pkg/errors"
)

// ValueType indicates the type of setting (string, bool, etc)
type ValueType byte

// We define a few types of setting we support.
// NB: We don't reuse sql/parser.Types here, to avoid deps which would then make
// `settings` unusable in a whole subtree of packages.
const (
	StringValue ValueType = 's'
	BoolValue             = 'b'
	IntValue              = 'i'
	FloatValue            = 'f'
)

func valueTypeFromStr(s string) (ValueType, error) {
	if len(s) != 1 {
		return 0, errors.Errorf("invalid value type str %q", s)
	}
	t := ValueType(s[0])
	switch t {
	case StringValue, BoolValue, IntValue, FloatValue:
		return t, nil
	}
	return 0, errors.Errorf("invalid value type char %c", t)
}

func parseRaw(raw string, typ ValueType) (value, error) {
	ret := value{typ: typ}
	var err error
	switch typ {
	case StringValue:
		ret.s = raw
	case BoolValue:
		ret.b, err = strconv.ParseBool(raw)
	case IntValue:
		ret.i, err = strconv.Atoi(raw)
	case FloatValue:
		ret.f, err = strconv.ParseFloat(raw, 64)
	default:
		err = errors.Errorf("invalid value type %c", typ)
	}
	return ret, err
}

// EncodeBool encodes a bool in the format parseRaw expects.
func EncodeBool(b bool) string {
	return strconv.FormatBool(b)
}

// EncodeInt encodes an int in the format parseRaw expects.
func EncodeInt(i int) string {
	return strconv.Itoa(i)
}

// EncodeFloat encodes a bool in the format parseRaw expects.
func EncodeFloat(f float64) string {
	return strconv.FormatFloat(f, 'E', -1, 64)
}
