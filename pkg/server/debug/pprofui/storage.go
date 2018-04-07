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

package pprofui

import (
	"io"
)

// Storage exposes the methods for storing and accessing profiles.
type Storage interface {
	// ID generates a unique ID for use in Store.
	ID() string
	// Store invokes the passed-in closure with a writer that stores its input.
	Store(id string, write func(io.Writer) error) error
	// Get invokes the passed-in closure with a reader for the data at the given id.
	// An error is returned when no data is found.
	Get(id string, read func(io.Reader) error) error
}
