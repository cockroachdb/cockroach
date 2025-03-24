// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

// Bad idea, delete and use the Encoder properly everywhere

/*

import (
	"bytes"
	"fmt"
)

var sep = []byte{'/'}

// Key is a component-decomposed key.
// Honor system: don't include `sep` in any component.
type Key [][]byte

func (k Key) String() string {
	return string(k.Encode())
}

func (k Key) Encode() []byte {
	return bytes.Join(k, sep)
}

func DecodeKey(b []byte) Key {
	return bytes.Split(b, sep)
}

func MakeKey(components ...any) Key {
	var sl Key
	for _, s := range components {
		sl = append(sl, []byte(fmt.Sprint(s)))
	}
	return sl

}
*/
