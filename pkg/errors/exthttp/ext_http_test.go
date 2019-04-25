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

package exthttp_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors"
	"github.com/cockroachdb/cockroach/pkg/errors/exthttp"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func TestHTTP(t *testing.T) {
	err := fmt.Errorf("hello")
	err = exthttp.WrapWithHTTPCode(err, 302)

	// Simulate a network transfer.
	enc := errors.EncodeError(err)
	otherErr := errors.DecodeError(enc)

	tt := testutils.T{T: t}

	// Error is preserved through the network.
	tt.CheckDeepEqual(otherErr, err)

	// It's possible to extract the HTTP code.
	tt.CheckEqual(exthttp.GetHTTPCode(otherErr, 100), 302)

	// If there are multiple codes, the most recent one wins.
	otherErr = exthttp.WrapWithHTTPCode(otherErr, 404)
	tt.CheckEqual(exthttp.GetHTTPCode(otherErr, 100), 404)
}
