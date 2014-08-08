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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package engine

import (
	"encoding/gob"
	"fmt"
)

// InvalidRangeMetaKeyError indicates that a Range Metadata key is somehow
// invalid.
type InvalidRangeMetaKeyError struct {
	Key Key
}

// NewInvalidRangeMetaKeyError returns a new InvalidRangeMetaKeyError
func NewInvalidRangeMetaKeyError(k Key) *InvalidRangeMetaKeyError {
	return &InvalidRangeMetaKeyError{k}
}

// Error formats error string.
func (i *InvalidRangeMetaKeyError) Error() string {
	return fmt.Sprintf("'%s' is not valid range metadata key.", string(i.Key))
}

// Init registers engine error types with Gob.
func init() {
	gob.Register(&InvalidRangeMetaKeyError{})
}
