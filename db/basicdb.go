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

import "sync"

// BasicDB a simple, in-memory key-value store.
type BasicDB struct {
	sync.RWMutex
	data map[string]interface{}
}

// NewBasicDB allocates and returns a new BasicDB object.
func NewBasicDB() *BasicDB {
	return &BasicDB{data: map[string]interface{}{}}
}

// Put sets the given key to the value provided.
func (b *BasicDB) Put(key string, val interface{}) error {
	b.Lock()
	defer b.Unlock()
	b.data[key] = val
	return nil
}

// Get returns the value for the given key, nil otherwise.
func (b *BasicDB) Get(key string) (interface{}, error) {
	b.RLock()
	defer b.RUnlock()
	return b.data[key], nil
}

// Delete removes the item from the db with the given key.
func (b *BasicDB) Delete(key string) error {
	b.Lock()
	defer b.Unlock()
	delete(b.data, key)
	return nil
}
