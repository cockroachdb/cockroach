// Copyright 2017 The Cockroach Authors.
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

package template

import (
	"bytes"
	"strings"
	"testing"
)

const (
	exampleTemplate = `
package d

func Prolog() {
}

// @for a

type A struct{}

func MakeA() A {
	return A{}
}

// @if b

// Conversion methods

// @fi b

// @for b

type B struct{}

func (A) ToB() B {
	return B{}
}

// @done b

// @done a

func Epilog() {
}
`

	expectedDump = `echo
echo	package d
echo
echo	func Prolog() {
echo	}
echo
for	a
	echo
	echo	type A struct{}
	echo
	echo	func MakeA() A {
	echo		return A{}
	echo	}
	echo
	if	b
		echo
		echo	// Conversion methods
		echo
	fi	b
	echo
	for	b
		echo
		echo	type B struct{}
		echo
		echo	func (A) ToB() B {
		echo		return B{}
		echo	}
		echo
	done	b
	echo
done	a
echo
echo	func Epilog() {
echo	}
`
)

func TestParseAndDump(t *testing.T) {
	tmpl, err := Parse(strings.NewReader(exampleTemplate))
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	tmpl.Dump(&b)
	if got := b.String(); got != expectedDump {
		t.Fatalf("expected %q but got %q", expectedDump, got)
	}
}
