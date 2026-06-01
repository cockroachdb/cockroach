// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	"google.golang.org/protobuf/proto"
)

// fakeGCPProjectsClient implements IGCPProjectsClient for testing.
type fakeGCPProjectsClient struct {
	project *computepb.Project
	err     error
}

func (f *fakeGCPProjectsClient) Get(
	_ context.Context, _ *computepb.GetProjectRequest, _ ...gax.CallOption,
) (*computepb.Project, error) {
	return f.project, f.err
}

// buildProjectWithSSHKeys creates a computepb.Project with the given ssh-keys
// metadata value.
func buildProjectWithSSHKeys(sshKeysValue string) *computepb.Project {
	return &computepb.Project{
		CommonInstanceMetadata: &computepb.Metadata{
			Items: []*computepb.Items{
				{
					Key:   proto.String("ssh-keys"),
					Value: proto.String(sshKeysValue),
				},
			},
		},
	}
}

const testED25519Key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHIlVVdl0uNPRvqRmJcxBQVFIBLTHyDZmZKNfbSnPDNv alice@laptop"

func testLogger() *logger.Logger {
	return logger.NewLogger("warn")
}

func TestGCPMetadataKeysProvider_GetAuthorizedKeys(t *testing.T) {
	tests := []struct {
		name       string
		sshKeys    string
		wantLines  int
		wantErr    string
		clientErr  error
		noMetadata bool
	}{
		{
			name: "valid keys parsed and sorted by user",
			sshKeys: "bob:" + testED25519Key + "\n" +
				"alice:" + testED25519Key,
			wantLines: 2,
		},
		{
			name: "malformed line without colon skipped",
			sshKeys: "no-colon-here\n" +
				"alice:" + testED25519Key,
			wantLines: 1,
		},
		{
			name: "invalid key skipped",
			sshKeys: "bob:not-a-valid-ssh-key\n" +
				"alice:" + testED25519Key,
			wantLines: 1,
		},
		{
			name: "empty lines skipped",
			sshKeys: "\n\nalice:" + testED25519Key +
				"\n\n",
			wantLines: 1,
		},
		{
			name:       "no ssh-keys metadata item",
			noMetadata: true,
			wantErr:    "no ssh-keys metadata found",
		},
		{
			name:    "all keys invalid",
			sshKeys: "bob:invalid\ncharlie:also-bad",
			wantErr: "no valid SSH keys found",
		},
		{
			name:      "API error propagated",
			clientErr: context.DeadlineExceeded,
			wantErr:   "fetch GCP project metadata",
		},
		{
			name: "reserved user root filtered out",
			sshKeys: "root:" + testED25519Key + "\n" +
				"alice:" + testED25519Key,
			wantLines: 1,
		},
		{
			name: "reserved user ubuntu filtered out",
			sshKeys: "ubuntu:" + testED25519Key + "\n" +
				"alice:" + testED25519Key,
			wantLines: 1,
		},
		{
			name: "only reserved users yields error",
			sshKeys: "root:" + testED25519Key + "\n" +
				"ubuntu:" + testED25519Key,
			wantErr: "no valid SSH keys found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var project *computepb.Project
			if tt.noMetadata {
				project = &computepb.Project{
					CommonInstanceMetadata: &computepb.Metadata{},
				}
			} else if tt.clientErr == nil {
				project = buildProjectWithSSHKeys(tt.sshKeys)
			}

			client := &fakeGCPProjectsClient{
				project: project,
				err:     tt.clientErr,
			}
			provider := NewGCPMetadataKeysProvider(
				client, "test-project", testLogger(),
			)

			result, err := provider.GetAuthorizedKeys(context.Background())

			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, result)

			lines := strings.Split(
				strings.TrimSpace(string(result)), "\n",
			)
			require.Len(t, lines, tt.wantLines)

			// Each output line must be a valid authorized_keys entry.
			for _, line := range lines {
				_, _, _, _, parseErr := ssh.ParseAuthorizedKey(
					[]byte(line),
				)
				require.NoError(t, parseErr,
					"output should be valid authorized_keys: %s", line)
			}
		})
	}
}

func TestParseGCPSSHKeys(t *testing.T) {
	l := testLogger()

	tests := []struct {
		name     string
		raw      string
		wantLen  int
		wantUser string
	}{
		{
			name:     "single valid key",
			raw:      "alice:" + testED25519Key,
			wantLen:  1,
			wantUser: "alice",
		},
		{
			name: "multiple valid keys",
			raw: "alice:" + testED25519Key + "\n" +
				"bob:" + testED25519Key,
			wantLen: 2,
		},
		{
			name:    "empty input",
			raw:     "",
			wantLen: 0,
		},
		{
			name:    "only malformed lines",
			raw:     "no-colon\nalso-bad",
			wantLen: 0,
		},
		{
			name:    "reserved user filtered",
			raw:     "root:" + testED25519Key,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, err := parseGCPSSHKeys(tt.raw, l)
			require.NoError(t, err)
			require.Len(t, keys, tt.wantLen)
			if tt.wantUser != "" && len(keys) > 0 {
				require.Equal(t, tt.wantUser, keys[0].user)
			}
		})
	}
}

func TestExtractSSHKeysMetadata(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		project := buildProjectWithSSHKeys("test-value")
		require.Equal(t, "test-value",
			extractSSHKeysMetadata(project))
	})

	t.Run("not found", func(t *testing.T) {
		project := &computepb.Project{
			CommonInstanceMetadata: &computepb.Metadata{
				Items: []*computepb.Items{
					{
						Key:   proto.String("other-key"),
						Value: proto.String("other-value"),
					},
				},
			},
		}
		require.Empty(t, extractSSHKeysMetadata(project))
	})

	t.Run("nil metadata", func(t *testing.T) {
		project := &computepb.Project{}
		require.Empty(t, extractSSHKeysMetadata(project))
	})
}
