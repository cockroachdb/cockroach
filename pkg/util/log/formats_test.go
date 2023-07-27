// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestFormatRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	// Make the test below deterministic.
	formatNames := make([]string, 0, len(formatters))
	for n := range formatters {
		formatNames = append(formatNames, n)
	}
	sort.Strings(formatNames)

	ctx := context.Background()
	ctx = logtags.AddTag(ctx, "a", "secret1")
	ctx = logtags.AddTag(ctx, "b", redact.Sprintf("safe1 %s", "secret2"))

	for _, formatName := range formatNames {
		t.Run(formatName, func(t *testing.T) {
			for _, redactable := range []bool{false, true} {
				t.Run(fmt.Sprintf("redactable=%v", redactable), func(t *testing.T) {
					for _, redactOut := range []bool{false, true} {
						t.Run(fmt.Sprintf("redact=%v", redactOut), func(t *testing.T) {
							subdir := filepath.Join(sc.logDir, formatName)
							require.NoError(t, os.MkdirAll(subdir, 0755))

							// Create a config that sends DEV to the subdirectory,
							// with the format being tested.
							config := logconfig.DefaultConfig()
							config.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
								"test": {
									FileDefaults: logconfig.FileDefaults{
										Dir: &subdir,
										CommonSinkConfig: logconfig.CommonSinkConfig{
											Format:     &formatName,
											Redactable: &redactable,
											Redact:     &redactOut,
										},
									},
									Channels: logconfig.SelectChannels(channel.DEV),
								},
							}
							config.CaptureFd2.Enable = false
							// Validate and apply the config.
							require.NoError(t, config.Validate(&sc.logDir))
							TestingResetActive()
							cleanupFn, err := ApplyConfig(config)
							require.NoError(t, err)
							defer cleanupFn()

							Infof(ctx, "safe2 %s", "secret3")
							Flush()

							contents, err := os.ReadFile(getDebugLogFileName(t))
							require.NoError(t, err)
							require.Greater(t, len(contents), 0)
							lastLineStart := bytes.LastIndexByte(contents[:len(contents)-1], '\n')
							require.Greater(t, lastLineStart, 0)
							lastLine := contents[lastLineStart:]

							t.Logf("%s", lastLine)

							// Expect the safe message regardless of redaction configuration.
							for i := 1; i <= 2; i++ {
								toFind := "safe" + strconv.Itoa(i)
								if !bytes.Contains(lastLine, []byte(toFind)) {
									t.Errorf("expected %q in string:\n%s", toFind, lastLine)
								}
							}

							if !redactOut {
								// Secrets should be preserved.
								for i := 1; i <= 3; i++ {
									toFind := "secret" + strconv.Itoa(i)
									if !bytes.Contains(lastLine, []byte(toFind)) {
										t.Errorf("expected %q in string:\n%s", toFind, lastLine)
									}
								}
							} else {
								// Secrets should be redacted out.
								if bytes.Contains(lastLine, []byte("secret")) {
									t.Errorf("secret not redacted:\n%s", lastLine)
								}
							}

							if redactable {
								// Output should still contain redaction markers.
								if redactOut {
									if !bytes.Contains(lastLine, redact.StartMarker()) {
										t.Errorf("markers missing from redactable output:\n%s", lastLine)
									}
								} else {
									if !bytes.Contains(lastLine, append(redact.StartMarker(), "secret"...)) {
										t.Errorf("secrets missing from redactable output:\n%s", lastLine)
									}
								}
							} else {
								// Output should escape redaction markers.
								if bytes.Contains(lastLine, redact.StartMarker()) ||
									bytes.Contains(lastLine, redact.EndMarker()) {
									t.Errorf("redaction marker not escaped:\n%s", lastLine)
								}
							}
						})
					}
				})
			}
		})
	}
}
