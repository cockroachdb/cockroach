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

package withstack_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/errors/withstack"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/kr/pretty"
)

func TestWithStack(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := withstack.WithStack(errors.New("hello"))
	t.Logf("start err: %# v", pretty.Formatter(origErr))

	// Show that there is indeed a stack trace.
	s, ok := origErr.(errbase.SafeDetailer)
	if !ok {
		t.Errorf("unexpected: error does not implement SafeDetailer")
	} else {
		details := s.SafeDetails()
		tt.Check(len(details) > 0 && strings.Contains(details[0], "withstack_test.go"))
	}

	enc := errbase.EncodeError(origErr)
	newErr := errbase.DecodeError(enc)

	// In any case, the library preserves the error message.
	tt.CheckEqual(newErr.Error(), origErr.Error())

	// The decoded error is marker-equal with the original one.
	tt.Check(markers.Is(newErr, origErr))

	// Also the new error includes the stack trace.
	s, ok = newErr.(errbase.SafeDetailer)
	if !ok {
		t.Errorf("unexpected: error does not implement SafeDetailer")
	} else {
		details := s.SafeDetails()
		tt.Check(len(details) > 0 && strings.Contains(details[0], "withstack_test.go"))
	}
}
