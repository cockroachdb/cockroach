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

	pkgErr "github.com/pkg/errors"
)

// SafeDetailer is an interface that can be implemented by errors that
// can provide PII-free additional strings suitable for reporting or
// telemetry.
type SafeDetailer interface {
	SafeDetails() []string
}

// GetAllSafeDetails collects the safe details from the given error object
// and all its causes.
func GetAllSafeDetails(err error) []SafeDetailPayload {
	var details []SafeDetailPayload
	for ; err != nil; err = UnwrapOnce(err) {
		details = append(details, GetSafeDetails(err))
	}
	return details
}

// GetSafeDetails collects the safe details from the given error
// object. If it is a wrapper, only the details from the wrapper are
// returned.
func GetSafeDetails(err error) (payload SafeDetailPayload) {
	payload.ErrorTypeName = FullTypeName(err)
	payload.SafeDetails = getDetails(err)
	return
}

func getDetails(err error) []string {
	if sd, ok := err.(SafeDetailer); ok {
		return sd.SafeDetails()
	}
	// For convenience, we also know how to extract stack traces
	// in the style of github.com/pkg/errors.
	if st, ok := err.(interface{ StackTrace() pkgErr.StackTrace }); ok {
		return []string{fmt.Sprintf("%+v", st.StackTrace())}
	}
	return nil
}

// SafeDetailPayload captures the safe strings for one
// level of wrapping.
type SafeDetailPayload struct {
	// ErrorTypeName is the type of the error that the details are coming from.
	ErrorTypeName string
	// SafeDetails are the PII-free strings.
	SafeDetails []string
}

// Fill can be used to concatenate multiple SafeDetailPayloads.
func (s *SafeDetailPayload) Fill(slice []string) []string {
	slice = append(slice, fmt.Sprintf("-- details for %s:", s.ErrorTypeName))
	slice = append(slice, s.SafeDetails...)
	return slice
}
