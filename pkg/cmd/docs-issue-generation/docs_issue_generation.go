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

import (
	"fmt"
	"time"
)

// the heart of the script to fetch and manipulate all data and create the individual docs issues
func docsIssueGeneration(params queryParameters) error {
	prs, err := searchCockroachPRs(params.StartTime, params.EndTime)
	if err != nil {
		return err
	}
	docsIssues, err := constructDocsIssues(prs)
	if err != nil {
		return err
	}
	if params.DryRun {
		fmt.Printf("Start time: %+v\n", params.StartTime.Format(time.RFC3339))
		fmt.Printf("End time: %+v\n", params.EndTime.Format(time.RFC3339))
		fmt.Printf("Number of PRs found: %d\n", len(prs))
		if len(docsIssues) > 0 {
			fmt.Printf("Dry run is enabled. The following %d docs issue(s) would be created:\n", len(docsIssues))
			fmt.Printf("%+v\n", docsIssues)
		} else {
			fmt.Println("No docs issues need to be created.")
		}
	} else {
		batchSize := 50
		for i := 0; i < len(docsIssues); i += batchSize {
			end := i + batchSize
			if end > len(docsIssues) {
				end = len(docsIssues)
			}
			batch := docsIssueBatch{
				IssueUpdates: docsIssues[i:end],
			}
			err := batch.createDocsIssuesInBulk()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func splitBySlashOrHash(r rune) bool {
	return r == '/' || r == '#'
}
