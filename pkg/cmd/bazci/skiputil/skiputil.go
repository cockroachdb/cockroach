// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package skiputil

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type Test struct {
	TestName       string `json:"test_name"`
	TestTargetName string `json:"test_target_name"`
}

type skippedTestsResponse struct {
	Data []Test `json:"data"`
}

const (
	budibaseAppID  = "app_crdbskippedtests_68cd68bbd82b48f6a9ecdbcab2c759df"
	budibaseApiKey = "dd5e309e317b7a138b1bd12a67539432-862e75e4d048b67a208786ad92b0465e32fdbb216cccf60ba3dc4009a615306921fdf2f872ff203369dce521643ede7d80"
	masterTableID  = "ta_3ee6617042d74894b472d244473f3536"
)

func GetSkippedTests() (map[Test]bool, error) {
	url := fmt.Sprintf("https://budibase.app/api/public/v1/tables/%s/rows/search", masterTableID)
	payload := strings.NewReader("{\"paginate\":false}")
	req, _ := http.NewRequest("POST", url, payload)
	req.Header.Add("accept", "application/json")
	req.Header.Add("x-budibase-app-id", budibaseAppID)
	req.Header.Add("content-type", "application/json")
	req.Header.Add("x-budibase-api-key", budibaseApiKey)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var tests skippedTestsResponse
	if err := json.Unmarshal(body, &tests); err != nil {
		log.Fatal(err)
	}
	// Create a set whose members are skipped tests.
	skippedTests := make(map[Test]bool, len(tests.Data))
	for _, test := range tests.Data {
		skippedTests[test] = true
	}
	return skippedTests, nil
}
