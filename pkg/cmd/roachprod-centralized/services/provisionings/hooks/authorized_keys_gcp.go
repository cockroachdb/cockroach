// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"sort"
	"strings"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/crypto/ssh"
)

// IGCPProjectsClient is the interface for fetching GCP project metadata. This
// enables injecting a mock in tests without making real API calls. The
// signature matches the real compute.ProjectsClient.Get method.
type IGCPProjectsClient interface {
	Get(ctx context.Context, req *computepb.GetProjectRequest, opts ...gax.CallOption) (*computepb.Project, error)
}

// NewGCPProjectsClient creates a production GCP projects REST client.
func NewGCPProjectsClient(ctx context.Context) (*compute.ProjectsClient, error) {
	return compute.NewProjectsRESTClient(ctx)
}

// reservedSSHUsers are users filtered out from GCP project metadata to match
// the behavior of pkg/roachprod/vm/gce/utils.go:isValidSSHUser. These users
// are infrastructure-managed and should not appear in authorized_keys.
var reservedSSHUsers = map[string]bool{
	"root":   true,
	"ubuntu": true,
}

// GCPMetadataKeysProvider fetches SSH public keys from GCP project metadata.
// It reads the "ssh-keys" metadata item, parses the "user:key" format, validates
// each key with ssh.ParseAuthorizedKey, and returns the result in
// authorized_keys format. Reserved users (root, ubuntu) are filtered out.
type GCPMetadataKeysProvider struct {
	client  IGCPProjectsClient
	project string
	l       *logger.Logger
}

// NewGCPMetadataKeysProvider creates a provider that fetches SSH keys from the
// given GCP project's metadata.
func NewGCPMetadataKeysProvider(
	client IGCPProjectsClient, project string, l *logger.Logger,
) *GCPMetadataKeysProvider {
	return &GCPMetadataKeysProvider{client: client, project: project, l: l}
}

// authorizedKey holds a parsed SSH public key with its associated user and
// comment.
type authorizedKey struct {
	user    string
	key     ssh.PublicKey
	comment string
}

// asSSHLine formats the key in authorized_keys format (key_type key_data
// comment).
func (k authorizedKey) asSSHLine() string {
	formatted := strings.TrimSuffix(
		string(ssh.MarshalAuthorizedKey(k.key)), "\n",
	)
	if k.comment != "" {
		return formatted + " " + k.comment
	}
	return formatted
}

// GetAuthorizedKeys fetches SSH public keys from GCP project metadata and
// returns them in authorized_keys format. Malformed lines, invalid keys,
// and reserved users are skipped with warnings. Keys are sorted by username
// for consistency.
func (p *GCPMetadataKeysProvider) GetAuthorizedKeys(ctx context.Context) ([]byte, error) {
	project, err := p.client.Get(ctx, &computepb.GetProjectRequest{
		Project: p.project,
	})
	if err != nil {
		return nil, errors.Wrapf(
			err, "fetch GCP project metadata for %q", p.project,
		)
	}

	raw := extractSSHKeysMetadata(project)
	if raw == "" {
		return nil, errors.Newf(
			"no ssh-keys metadata found in GCP project %q", p.project,
		)
	}

	keys, err := parseGCPSSHKeys(raw, p.l)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, errors.Newf(
			"no valid SSH keys found in GCP project %q metadata",
			p.project,
		)
	}

	// Sort by username for consistency. SliceStable preserves the original
	// order for keys with the same username.
	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i].user < keys[j].user
	})

	var buf bytes.Buffer
	for _, k := range keys {
		buf.WriteString(k.asSSHLine() + "\n")
	}
	return buf.Bytes(), nil
}

// extractSSHKeysMetadata finds the "ssh-keys" item in the project's common
// instance metadata.
func extractSSHKeysMetadata(project *computepb.Project) string {
	if project.GetCommonInstanceMetadata() == nil {
		return ""
	}
	for _, item := range project.GetCommonInstanceMetadata().GetItems() {
		if item.GetKey() == "ssh-keys" {
			return item.GetValue()
		}
	}
	return ""
}

// parseGCPSSHKeys parses the GCP metadata ssh-keys value. Each line has the
// format "user:key_type key_data comment". Lines that are empty, malformed
// (no colon), contain invalid SSH keys, or belong to reserved users are
// skipped with warnings.
func parseGCPSSHKeys(raw string, l *logger.Logger) ([]authorizedKey, error) {
	var keys []authorizedKey
	scanner := bufio.NewScanner(strings.NewReader(raw))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		colonIdx := strings.IndexRune(line, ':')
		if colonIdx == -1 {
			l.Warn("skipping malformed GCP metadata line (no colon)",
				slog.String("line", line),
			)
			continue
		}

		user := line[:colonIdx]
		keyStr := line[colonIdx+1:]

		if reservedSSHUsers[user] {
			l.Warn("skipping reserved SSH user in GCP metadata",
				slog.String("user", user),
			)
			continue
		}

		pubKey, comment, _, _, err := ssh.ParseAuthorizedKey([]byte(keyStr))
		if err != nil {
			l.Warn("skipping invalid SSH key in GCP metadata",
				slog.String("user", user),
				slog.Any("error", err),
			)
			continue
		}

		keys = append(keys, authorizedKey{
			user:    user,
			key:     pubKey,
			comment: comment,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(
			err, "scan GCP metadata ssh-keys",
		)
	}
	return keys, nil
}
