// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)

package engine

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/util/keys"
)

// EncodedKey is an encoded key, distinguished from Key in that it is
// an encoded version.
type EncodedKey []byte

// Next returns the next key in lexicographic sort order.
func (k EncodedKey) Next() EncodedKey {
	return EncodedKey(keys.BytesNext(k))
}

// Less compares two keys.
func (k EncodedKey) Less(l EncodedKey) bool {
	return bytes.Compare(k, l) < 0
}

// Equal returns whether two keys are identical.
func (k EncodedKey) Equal(l EncodedKey) bool {
	return bytes.Equal(k, l)
}

// String returns a string-formatted version of the key.
func (k EncodedKey) String() string {
	return fmt.Sprintf("%q", []byte(k))
}

