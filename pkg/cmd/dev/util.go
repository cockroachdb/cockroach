// Copyright 2020 The Cockroach Authors.
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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/spf13/cobra"
)

func mustGetFlagString(cmd *cobra.Command, name string) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		log.Fatalf(context.Background(), "unexpected error: %v", err)
	}
	return val
}

func mustGetFlagBool(cmd *cobra.Command, name string) bool {
	val, err := cmd.Flags().GetBool(name)
	if err != nil {
		log.Fatalf(context.Background(), "unexpected error: %v", err)
	}
	return val
}

func mustGetFlagDuration(cmd *cobra.Command, name string) time.Duration {
	val, err := cmd.Flags().GetDuration(name)
	if err != nil {
		log.Fatalf(context.Background(), "unexpected error: %v", err)
	}
	return val
}

func execute(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	log.Infof(ctx, "executing: %s", log.Safe(cmd.String()))

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return err
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		log.Infof(ctx, "-- %s\n", redact.Safe(line))
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

func getPathToBin(target string) (string, error) {
	// actionQueryResult is used to unmarshal the results of the bazel action
	// query.
	type actionQueryResult struct {
		Artifacts []struct {
			ID       string `json:"id"`
			ExecPath string `json:"execPath"`
		} `json:"artifacts"`

		Actions []struct {
			Mnemonic  string   `json:"mnemonic"`
			OutputIds []string `json:"outputIds"`
		} `json:"actions"`
	}

	buf, err := exec.Command("bazel", "aquery", target, "--output=jsonproto").Output()
	if err != nil {
		return "", err
	}

	var result actionQueryResult
	if err := json.Unmarshal(buf, &result); err != nil {
		return "", err
	}

	const binaryMnemomic = "GoLink"
	for _, action := range result.Actions {
		if action.Mnemonic == binaryMnemomic {
			id := action.OutputIds[0]
			for _, artifact := range result.Artifacts {
				if artifact.ID == id {
					return artifact.ExecPath, nil
				}
			}
		}
	}

	return "", fmt.Errorf("could not find path to binary %q", target)
}
