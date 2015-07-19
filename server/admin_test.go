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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// getText fetches the HTTP response body as text in the form of a
// byte slice from the specified URL.
func getText(url string) ([]byte, error) {
	client, err := testutils.NewTestHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// getJSON fetches the JSON from the specified URL and returns
// it as unmarshaled JSON. Returns an error on any failure to fetch
// or unmarshal response body.
func getJSON(url string) (interface{}, error) {
	body, err := getText(url)
	if err != nil {
		return nil, err
	}
	var jI interface{}
	if err := json.Unmarshal(body, &jI); err != nil {
		return nil, err
	}
	return jI, nil
}

// TestAdminDebugExpVar verifies that cmdline and memstats variables are
// available via the /debug/vars link.
func TestAdminDebugExpVar(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()

	jI, err := getJSON(s.Ctx.RequestScheme() + "://" + s.ServingAddr() + debugEndpoint + "vars")
	if err != nil {
		t.Fatalf("failed to fetch JSON: %v", err)
	}
	j := jI.(map[string]interface{})
	if _, ok := j["cmdline"]; !ok {
		t.Error("cmdline not found in JSON response")
	}
	if _, ok := j["memstats"]; !ok {
		t.Error("memstats not found in JSON response")
	}
}

// TestAdminDebugPprof verifies that pprof tools are available.
// via the /debug/pprof/* links.
func TestAdminDebugPprof(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()

	if body, err := getText(s.Ctx.RequestScheme() + "://" + s.ServingAddr() + debugEndpoint + "pprof/block"); err != nil {
		t.Fatal(err)
	} else if exp := "contention:\ncycles/second="; !bytes.Contains(body, []byte(exp)) {
		t.Errorf("expected %s to contain %s", body, exp)
	}
}

// TestSetZoneInvalid sets invalid zone configs and verifies error
// responses.
func TestSetZoneInvalid(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()

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

	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	for i, test := range testData {
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s%s/%s", s.Ctx.RequestScheme(), s.ServingAddr(), zonePathPrefix, "foo"), strings.NewReader(test.zone))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Add(util.ContentTypeHeader, util.YAMLContentType)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode == http.StatusOK {
			t.Errorf("%d: expected error", i)
		} else if re := regexp.MustCompile(test.expErr); !re.MatchString(string(b)) {
			t.Errorf("%d: expected error matching %q; got %s", i, test.expErr, b)
		}
	}
}
