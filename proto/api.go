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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import "github.com/cockroachdb/cockroach/util"

// IsEmpty returns true if the client command ID has zero values.
func (ccid ClientCmdID) IsEmpty() bool {
	return ccid.WallTime == 0 && ccid.Random == 0
}

// Request is an interface for RPC requests.
type Request interface {
	// Header returns the request header.
	Header() *RequestHeader
}

// Response is an interface for RPC responses.
type Response interface {
	// Header returns the response header.
	Header() *ResponseHeader
	// Verify verifies response integrity, as applicable.
	Verify(req Request) error
}

// Header implements the Request interface for RequestHeader.
func (rh *RequestHeader) Header() *RequestHeader {
	return rh
}

// Header implements the Response interface for ResponseHeader.
func (rh *ResponseHeader) Header() *ResponseHeader {
	return rh
}

// Verify implements the Response interface for ResopnseHeader with a
// default noop. Individual response types should override this method
// if they contain checksummed data which can be verified.
func (rh *ResponseHeader) Verify(req Request) error {
	return nil
}

// GoError returns the non-nil error from the proto.Error union.
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
	case rh.Error.TransactionStatus != nil:
		return rh.Error.TransactionStatus
	case rh.Error.TransactionRetry != nil:
		return rh.Error.TransactionRetry
	case rh.Error.WriteIntent != nil:
		return rh.Error.WriteIntent
	case rh.Error.WriteTooOld != nil:
		return rh.Error.WriteTooOld
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
	case *TransactionStatusError:
		rh.Error = &Error{TransactionStatus: t}
	case *TransactionRetryError:
		rh.Error = &Error{TransactionRetry: t}
	case *WriteIntentError:
		rh.Error = &Error{WriteIntent: t}
	case *WriteTooOldError:
		rh.Error = &Error{WriteTooOld: t}
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

// Verify verifies the integrity of the get response value.
func (gr *GetResponse) Verify(req Request) error {
	if gr.Value != nil {
		return gr.Value.Verify(req.Header().Key)
	}
	return nil
}

// Verify verifies the integrity of the conditional put response's
// actual value, if not nil.
func (cpr *ConditionalPutResponse) Verify(req Request) error {
	if cpr.ActualValue != nil {
		return cpr.ActualValue.Verify(req.Header().Key)
	}
	return nil
}

// Verify verifies the integrity of every value returned in the scan.
func (sr *ScanResponse) Verify(req Request) error {
	for _, kv := range sr.Rows {
		if err := kv.Value.Verify(kv.Key); err != nil {
			return err
		}
	}
	return nil
}
