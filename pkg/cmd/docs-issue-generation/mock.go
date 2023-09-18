// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "fmt"

func (t *testingJiraGitHubClient) getValidEpicRef(issueKey string) (bool, string, error) {
	_, ok := t.GetValidEpicRefMap[issueKey]
	if !ok {
		return false, "", fmt.Errorf("issueKey %s not found in epicMap", issueKey)
	}
	return t.GetValidEpicRefMap[issueKey].IsEpic, t.GetValidEpicRefMap[issueKey].EpicKey, nil
}
