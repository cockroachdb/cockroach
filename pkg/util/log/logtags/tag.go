// Copyright 2018 The Cockroach Authors.
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

package logtags

import "fmt"

// Tag is a log tag, which has a string key and an arbitrary value.
// The value must support testing for equality. It can be nil.
type Tag struct {
	key   string
	value interface{}
}

// Key returns the key.
func (t *Tag) Key() string {
	return t.key
}

// Value returns the value.
func (t *Tag) Value() interface{} {
	return t.value
}

// ValueStr returns the value as a string.
func (t *Tag) ValueStr() string {
	if t.value == nil {
		return ""
	}
	switch v := t.value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprint(t.value)
	}
}
