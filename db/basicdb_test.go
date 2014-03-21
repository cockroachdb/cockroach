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
		val, err := store.Get(c.key)
		if err != nil {
			t.Errorf("get: expected no error, but got %s", err)
		}
		if val != nil {
			t.Errorf("expected key value %s to be nil: got %+v", c.key, val)
		}
		err = store.Put(c.key, c.value)
		if err != nil {
			t.Errorf("put: expected no error, but got %s", err)
		}
		val, err = store.Get(c.key)
		if err != nil {
			t.Errorf("get: expected no error, but got %s", err)
		}
		if val != c.value {
			t.Errorf("expected key value %s to be %+v: got %+v", val)
		}
		err = store.Delete(c.key)
		if err != nil {
			t.Errorf("delete: expected no error, but got %s", err)
		}
		val, err = store.Get(c.key)
		if err != nil {
			t.Errorf("get: expected no error, but got %s", err)
		}
		if val != nil {
			t.Errorf("expected key value %s to be nil: got %+v", c.key, val)
		}
	}
}
