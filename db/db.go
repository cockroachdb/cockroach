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

// Package db provides an interface and implementations for key-value stores.
package db

// DB is the interface that wraps the core operations of a key/value store.
type DB interface {
	// Put sets the given key to the value provided.
	Put(string, interface{})
	// Get returns the value for the given key, nil otherwise.
	Get(string) interface{}
	// Delete removes the item from the db with the given key.
	Delete(key string)
}
