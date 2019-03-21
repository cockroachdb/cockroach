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

package pgerror

// withMessage adds a message prefix.
type withMessage struct {
	cause error
	msg   string
}

// withDefaultCode decorates an error with a default pg error code.  This
// code is only used if the chain of causes underneath does not define
// a code yet, or if its derived code as per GetCode() is
// CodeUncategorizedError.
type withDefaultCode struct {
	cause error
	code  string
}

// withHint adds a pg error hint.
type withHint struct {
	cause error
	hint  string
}

// withDetail adds a pg error detail.
type withDetail struct {
	cause  error
	detail string
}

// withSource adds a pg error source.
type withSource struct {
	cause  error
	source *Error_Source
}

// withTelemetryKey adds a key for telemetry.
type withTelemetryKey struct {
	cause error
	key   string
}

// withSafeDetail adds a reportable (PII-free) layer of safe details.
type withSafeDetail struct {
	cause  error
	detail *SafeDetailPayload
}

// withUnknownErrorPayload indicates that an error wrapper received
// from the network is not understood on this host and was
// discarded. It is a sort of internal error and gets turned into an
// actual internal error through Flatten().
type withUnknownErrorPayload struct {
	cause       error
	payloadType string
}

// withInternalError is a general purpose internal error marker and
// gets turned into an Error internal error when flattened. It
// differs from withDefaultCode in that its code overrides the code of
// its cause.
type withInternalError struct {
	cause error
	// internalErrorCode decides the final code to use for the internal
	// error. If left empty, the value CodeInternalError is used during
	// Flatten() and GetCode(); but can be some of the others too
	// (e.g. XX001 corrupted data).
	internalErrorCode string
}
