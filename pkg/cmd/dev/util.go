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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
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

func executeReturningStdout(ctx context.Context, name string, args ...string) (*bytes.Buffer, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(stdout); err != nil {
		return nil, err
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}
	return buf, nil
}

func getPathToGoBin(ctx context.Context, target string) (string, error) {
	// We could pass --output=proto instead, but then we'd have to import
	// the generated Go code for the proto definition. For now, until we're
	// comfortable depending on that, just parse the JSON instead.
	buf, err := executeReturningStdout(ctx, "bazel", "aquery", target, "--output=jsonproto")
	if err != nil {
		return "", err
	}

	m := make(map[string]interface{})
	json.Unmarshal(buf.Bytes(), &m)

	// Find the action corresponding to the final result binary (its
	// mnemonic is "GoLink").
	for _, action := range m["actions"].([]interface{}) {
		action := action.(map[string]interface{})
		if action["mnemonic"] == "GoLink" {
			// The path is the output with this output ID.
			outputIdStr := action["outputIds"].([]interface{})[0].(string)
			outputId, err := strconv.Atoi(outputIdStr)
			if err != nil {
				return "", nil
			}
			return m["artifacts"].([]interface{})[outputId].(map[string]interface{})["execPath"].(string), nil
		}
	}

	return "", fmt.Errorf("could not find path to binary %q", target)
}
