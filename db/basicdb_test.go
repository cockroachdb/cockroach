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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)

package db

import "testing"

func TestBasicDBPutGetDelete(t *testing.T) {
	store := NewBasicDB()
	testCases := []struct {
		key   string
		value interface{}
	}{
		{"dog", "woof"},
		{"cat", "meow"},
		{"server", 42},
	}
	for _, c := range testCases {
		if store.Get(c.key) != nil {
			t.Errorf("expected key value %s to be nil: got %+v", c.key, store.Get(c.key))
		}
		store.Put(c.key, c.value)
		if store.Get(c.key) != c.value {
			t.Errorf("expected key value %s to be %+v: got %+v", store.Get(c.key))
		}
		store.Delete(c.key)
		if store.Get(c.key) != nil {
			t.Errorf("expected key value %s to be nil: got %+v", c.key, store.Get(c.key))
		}
	}
}
