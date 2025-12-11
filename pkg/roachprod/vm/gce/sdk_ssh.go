// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/ssh"
	"google.golang.org/protobuf/proto"
)

// GetUserAuthorizedKeysWithContext fetches SSH authorized keys from GCP project metadata using the SDK.
// This matches the gcloud implementation which retrieves project metadata and parses the ssh-keys field.
func (p *Provider) GetUserAuthorizedKeysWithContext(ctx context.Context) (AuthorizedKeys, error) {
	// Get project metadata
	req := &computepb.GetProjectRequest{
		Project: p.metadataProject,
	}

	project, err := p.computeProjectsClient.Get(ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get project metadata for %s", p.metadataProject)
	}

	// Find the ssh-keys metadata item
	var sshKeysValue string
	if project.CommonInstanceMetadata != nil {
		for _, item := range project.CommonInstanceMetadata.Items {
			if item.GetKey() == "ssh-keys" {
				sshKeysValue = item.GetValue()
				break
			}
		}
	}

	// Parse the ssh-keys value into AuthorizedKeys
	// Format matches the gcloud version: each line is username:ssh-key-content
	var authorizedKeys AuthorizedKeys
	scanner := bufio.NewScanner(bytes.NewBufferString(sshKeysValue))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		// N.B. Below, we skip over invalid public keys as opposed to failing. Since we don't control how these keys are
		// uploaded, it's possible for a key to become invalid.
		// N.B. This implies that an operation like `AddUserAuthorizedKey` has the side effect of removing invalid
		// keys, since they are skipped here, and the result is then uploaded via `SetUserAuthorizedKeys`.
		colonIdx := strings.IndexRune(line, ':')
		if colonIdx == -1 {
			fmt.Fprintf(os.Stderr, "WARN: malformed public key line %q\n", line)
			continue
		}

		user := line[:colonIdx]
		key := line[colonIdx+1:]

		if !isValidSSHUser(user) {
			continue
		}

		pubKey, comment, _, _, err := ssh.ParseAuthorizedKey([]byte(key))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: failed to parse public key in project metadata: %v\n%q\n", err, key)
			continue
		}
		authorizedKeys = append(authorizedKeys, AuthorizedKey{User: user, Key: pubKey, Comment: comment})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read public keys from project metadata: %w", err)
	}

	// For consistency, return keys sorted by username.
	sort.Slice(authorizedKeys, func(i, j int) bool {
		return authorizedKeys[i].User < authorizedKeys[j].User
	})

	return authorizedKeys, nil
}

// SetUserAuthorizedKeysWithContext updates SSH authorized keys in GCP project metadata using the SDK.
// This matches the gcloud implementation which updates the project metadata with the ssh-keys field.
func (p *Provider) SetUserAuthorizedKeysWithContext(
	ctx context.Context, keys AuthorizedKeys,
) error {
	// Build the ssh-keys value from AuthorizedKeys using the AsProjectMetadata() method
	sshKeysValue := string(keys.AsProjectMetadata())
	// Remove trailing newline if present
	sshKeysValue = strings.TrimSuffix(sshKeysValue, "\n")

	// Get the current project metadata to retrieve the fingerprint
	getReq := &computepb.GetProjectRequest{
		Project: p.metadataProject,
	}

	project, err := p.computeProjectsClient.Get(ctx, getReq)
	if err != nil {
		return errors.Wrapf(err, "failed to get project metadata for %s", p.metadataProject)
	}

	// Build the new metadata
	// We need to preserve existing metadata items that aren't ssh-keys
	var items []*computepb.Items
	if project.CommonInstanceMetadata != nil {
		for _, item := range project.CommonInstanceMetadata.Items {
			if item.GetKey() != "ssh-keys" {
				items = append(items, item)
			}
		}
	}

	// Add the ssh-keys item
	items = append(items, &computepb.Items{
		Key:   proto.String("ssh-keys"),
		Value: proto.String(sshKeysValue),
	})

	// Build the metadata resource
	metadata := &computepb.Metadata{
		Items: items,
	}

	// If we have an existing fingerprint, include it to ensure we're updating the latest version
	if project.CommonInstanceMetadata != nil && project.CommonInstanceMetadata.Fingerprint != nil {
		metadata.Fingerprint = project.CommonInstanceMetadata.Fingerprint
	}

	// Update the project metadata
	updateReq := &computepb.SetCommonInstanceMetadataProjectRequest{
		Project:          p.metadataProject,
		MetadataResource: metadata,
	}

	op, err := p.computeProjectsClient.SetCommonInstanceMetadata(ctx, updateReq)
	if err != nil {
		return errors.Wrapf(err, "failed to update project metadata for %s", p.metadataProject)
	}

	// Wait for the operation to complete
	if err := op.Wait(ctx); err != nil {
		return errors.Wrapf(err, "failed to wait for metadata update for %s", p.metadataProject)
	}

	// Check for operation errors
	if opErr := op.Proto().GetError(); opErr != nil {
		return errors.Newf("metadata update failed for %s: %s", p.metadataProject, opErr.String())
	}

	return nil
}
