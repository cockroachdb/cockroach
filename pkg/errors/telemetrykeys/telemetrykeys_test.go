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

package telemetrykeys_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/errors/telemetrykeys"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/pkg/errors"
)

func TestTelemetry(t *testing.T) {
	tt := testutils.T{T: t}

	baseErr := errors.New("world")
	err := errors.Wrap(
		telemetrykeys.WithTelemetry(
			telemetrykeys.WithTelemetry(
				baseErr,
				"a", "b"),
			"b", "c"),
		"hello")

	tt.Check(markers.Is(err, baseErr))
	tt.CheckEqual(err.Error(), "hello: world")

	keys := telemetrykeys.GetTelemetryKeys(err)
	sort.Strings(keys)
	tt.CheckDeepEqual(keys, []string{"a", "b", "c"})

	errV := fmt.Sprintf("%+v", err)
	tt.Check(strings.Contains(errV, `telemetry keys: [a b]`))
	tt.Check(strings.Contains(errV, `telemetry keys: [b c]`))

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Check(markers.Is(newErr, baseErr))
	tt.CheckEqual(newErr.Error(), "hello: world")

	keys = telemetrykeys.GetTelemetryKeys(newErr)
	sort.Strings(keys)
	tt.CheckDeepEqual(keys, []string{"a", "b", "c"})

	errV = fmt.Sprintf("%+v", newErr)
	tt.Check(strings.Contains(errV, `telemetry keys: [a b]`))
	tt.Check(strings.Contains(errV, `telemetry keys: [b c]`))
}
