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

package server

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestSetZoneInvalid sets invalid zone configs and verifies error
// responses.
func TestSetZoneInvalid(t *testing.T) {
	defer leaktest.AfterTest(t)
	url, stopper := startAdminServer()
	defer stopper.Stop()

	testData := []struct {
		zone   string
		expErr string
	}{
		{`
replicas:
  - attrs: [dc1, ssd]
range_min_bytes: 128
range_max_bytes: 524288
`, "RangeMaxBytes 524288 less than minimum allowed"},
		{`
replicas:
  - attrs: [dc1, ssd]
range_min_bytes: 67108864
range_max_bytes: 67108864
`, "RangeMinBytes 67108864 is greater than or equal to RangeMaxBytes 67108864"},
		{`
range_min_bytes: 1048576
range_max_bytes: 67108864
`, "attributes for at least one replica must be specified in zone config"},
	}

	for i, test := range testData {
		re := regexp.MustCompile(test.expErr)
		req, err := http.NewRequest("POST", fmt.Sprintf("%s%s/%s", url, zonePathPrefix, "foo"), strings.NewReader(test.zone))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Add(util.ContentTypeHeader, util.YAMLContentType)
		_, err = sendAdminRequest(testContext, req)
		if err == nil {
			t.Errorf("%d: expected error", i)
		} else if !re.MatchString(err.Error()) {
			t.Errorf("%d: expected error matching %q; got %s", i, test.expErr, err)
		}
	}
}
