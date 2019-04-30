// Copyright 2019 The Cockroach Authors.
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

package errbase

import (
	"fmt"

	"github.com/gogo/protobuf/types"
)

// opaqueLeaf is used when receiving an unknown leaf type.
// Its important property is that if it is communicated
// back to some network system that _does_ know about
// the type, the original object can be restored.
type opaqueLeaf struct {
	msg         string
	typeName    TypeName
	safeDetails []string
	payload     *types.Any
}

// opaqueWrapper is used when receiving an unknown wrapper type.
// Its important property is that if it is communicated
// back to some network system that _does_ know about
// the type, the original object can be restored.
type opaqueWrapper struct {
	cause       error
	prefix      string
	typeName    TypeName
	safeDetails []string
	payload     *types.Any
}

// the opaque error types are errors too.

func (e *opaqueLeaf) Error() string { return e.msg }

func (e *opaqueWrapper) Error() string {
	if e.prefix == "" {
		return e.cause.Error()
	}
	return fmt.Sprintf("%s: %s", e.prefix, e.cause)
}

// the opaque wrapper is a wrapper.
func (e *opaqueWrapper) Cause() error  { return e.cause }
func (e *opaqueWrapper) Unwrap() error { return e.cause }

// they implement the safedetailer interface too.
func (e *opaqueLeaf) SafeDetails() []string    { return e.safeDetails }
func (e *opaqueWrapper) SafeDetails() []string { return e.safeDetails }
