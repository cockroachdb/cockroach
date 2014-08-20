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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import "github.com/cockroachdb/cockroach/util"

// IsEmpty returns true if the client command ID has zero values.
func (ccid ClientCmdID) IsEmpty() bool {
	return ccid.WallTime == 0 && ccid.Random == 0
}

// Request is an interface providing access to all requests'
// header structs.
type Request interface {
	Header() *RequestHeader
}

// Response is an interface providing access to all responses' header
// structs.
type Response interface {
	Header() *ResponseHeader
}

// Implementation of Request for RequestHeader.
func (rh *RequestHeader) Header() *RequestHeader {
	return rh
}

// Implementation of Response for ResponseHeader.
func (rh *ResponseHeader) Header() *ResponseHeader {
	return rh
}

// GoError converts the Error field of the response header to a
// GenericError.
func (rh *ResponseHeader) GoError() error {
	if rh.Error == nil {
		return nil
	}
	switch {
	case rh.Error.Generic != nil:
		return rh.Error.Generic
	case rh.Error.NotLeader != nil:
		return rh.Error.NotLeader
	case rh.Error.RangeNotFound != nil:
		return rh.Error.RangeNotFound
	case rh.Error.RangeKeyMismatch != nil:
		return rh.Error.RangeKeyMismatch
	default:
		return nil
	}
}

// SetGoError converts the specified type into either one of the proto-
// defined error types or into a GenericError for all other Go errors.
func (rh *ResponseHeader) SetGoError(err error) {
	if err == nil {
		rh.Error = nil
		return
	}
	switch t := err.(type) {
	case *NotLeaderError:
		rh.Error = &Error{NotLeader: t}
	case *RangeNotFoundError:
		rh.Error = &Error{RangeNotFound: t}
	case *RangeKeyMismatchError:
		rh.Error = &Error{RangeKeyMismatch: t}
	default:
		var canRetry bool
		if r, ok := err.(util.Retryable); ok {
			canRetry = r.CanRetry()
		}
		rh.Error = &Error{
			Generic: &GenericError{
				Message:   err.Error(),
				Retryable: canRetry,
			},
		}
	}
}
