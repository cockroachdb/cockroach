// Copyright 2015 The Cockroach Authors.
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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import (
	"go/constant"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

func TestNumericConstantAvailableTypes(t *testing.T) {
	wantInt := numValAvailIntFloatDec
	wantFloatButCanBeInt := numValAvailFloatIntDec
	wantFloat := numValAvailFloatDec

	testCases := []struct {
		str   string
		avail []Datum
	}{
		{"1", wantInt},
		{"0", wantInt},
		{"-1", wantInt},
		{"9223372036854775807", wantInt},
		{"1.0", wantFloatButCanBeInt},
		{"-1234.0000", wantFloatButCanBeInt},
		{"1.1", wantFloat},
		{"-1231.131", wantFloat},
		{"876543234567898765436787654321", wantFloat},
	}

	for i, test := range testCases {
		tok := token.INT
		if strings.Contains(test.str, ".") {
			tok = token.FLOAT
		}
		val := constant.MakeFromLiteral(test.str, tok, 0)
		if val.Kind() == constant.Unknown {
			t.Fatalf("%d: could not parse value string %q", i, test.str)
		}

		// Check available types.
		c := &NumVal{Value: val}
		avail := c.AvailableTypes()
		if !reflect.DeepEqual(avail, test.avail) {
			t.Errorf("%d: expected the available type set %v for %v, found %v", i, test.avail, c.Value.ExactString(), avail)
		}

		// Make sure it can be resolved as each of those types.
		for _, availType := range avail {
			if _, err := c.ResolveAsType(availType); err != nil {
				t.Errorf("%d: expected resolving %v as available type %s would succeed, found %v", i, c.Value.ExactString(), availType.Type(), err)
			}
		}
	}
}

func TestStringConstantAvailableTypes(t *testing.T) {
	wantStringButCanBeBytes := strValAvailStringBytes
	wantBytesButCanBeString := strValAvailBytesString
	wantBytes := strValAvailBytes

	testCases := []struct {
		c     *StrVal
		avail []Datum
	}{
		{&StrVal{"abc 世界", false}, wantStringButCanBeBytes},
		{&StrVal{"abc 世界", true}, wantBytesButCanBeString},
		{&StrVal{string([]byte{0xff, 0xfe, 0xfd}), true}, wantBytes},
	}

	for i, test := range testCases {
		// Check available types.
		avail := test.c.AvailableTypes()
		if !reflect.DeepEqual(avail, test.avail) {
			t.Errorf("%d: expected the available type set %v for %+v, found %v", i, test.avail, test.c, avail)
		}

		// Make sure it can be resolved as each of those types.
		for _, availType := range avail {
			if _, err := test.c.ResolveAsType(availType); err != nil {
				t.Errorf("%d: expected resolving %v as available type %s would succeed, found %v", i, test.c, availType.Type(), err)
			}
		}
	}
}
