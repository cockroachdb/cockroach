// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/errors"
	googleapioption "google.golang.org/api/option"
)

const sshKeysObjectName = "ssh-keys"

// SSHKeysBucketForProject returns the GCS bucket used to store the shared SSH
// key pool for project. Only the standard production and staging metadata
// projects use GCS; custom metadata projects retain the legacy GCE
// project-metadata backend.
func SSHKeysBucketForProject(project string) (string, bool) {
	if !usesGCSForSSHKeys(project) {
		return "", false
	}
	return "roachprod-ssh-keys-" + project, true
}

func usesGCSForSSHKeys(metadataProject string) bool {
	return metadataProject == DefaultProjectID || metadataProject == StagingProjectID
}

type sshKeysObjectStore interface {
	ReadObject(ctx context.Context, bucket, object string) ([]byte, error)
	WriteObject(ctx context.Context, bucket, object string, contents []byte) error
	Close() error
}

type gcsSSHKeysObjectStore struct {
	client *storage.Client
}

func (s *gcsSSHKeysObjectStore) ReadObject(
	ctx context.Context, bucket, object string,
) (contents []byte, retErr error) {
	reader, err := s.client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.CombineErrors(retErr, reader.Close()) }()
	return io.ReadAll(reader)
}

func (s *gcsSSHKeysObjectStore) WriteObject(
	ctx context.Context, bucket, object string, contents []byte,
) (retErr error) {
	writer := s.client.Bucket(bucket).Object(object).NewWriter(ctx)
	defer func() { retErr = errors.CombineErrors(retErr, writer.Close()) }()
	_, err := writer.Write(contents)
	return err
}

func (s *gcsSSHKeysObjectStore) Close() error {
	return s.client.Close()
}

var newSSHKeysObjectStore = func(ctx context.Context) (sshKeysObjectStore, error) {
	creds, _, err := roachprodutil.GetGCECredentials(
		ctx, roachprodutil.IAPTokenSourceOptions{},
	)
	if err != nil {
		return nil, errors.Wrap(err, "get GCE credentials for SSH key storage")
	}
	client, err := storage.NewClient(ctx, googleapioption.WithCredentials(creds))
	if err != nil {
		return nil, errors.Wrap(err, "construct GCS client for SSH key storage")
	}
	return &gcsSSHKeysObjectStore{client: client}, nil
}

// GetUserAuthorizedKeysFromGCS reads and parses the shared SSH key pool for
// one of the standard roachprod metadata projects.
func GetUserAuthorizedKeysFromGCS(
	ctx context.Context, project string,
) (_ AuthorizedKeys, _ []AuthorizedKeyParseIssue, retErr error) {
	bucket, ok := SSHKeysBucketForProject(project)
	if !ok {
		return nil, nil, errors.Newf(
			"GCS SSH key storage is not configured for metadata project %q", project,
		)
	}
	store, err := newSSHKeysObjectStore(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { retErr = errors.CombineErrors(retErr, store.Close()) }()

	contents, err := store.ReadObject(ctx, bucket, sshKeysObjectName)
	if err != nil {
		return nil, nil, errors.Wrapf(
			err, "read SSH keys from gs://%s/%s", bucket, sshKeysObjectName,
		)
	}
	keys, issues, err := ParseUserAuthorizedKeys(string(contents))
	if err != nil {
		return nil, issues, errors.Wrapf(
			err, "parse SSH keys from gs://%s/%s", bucket, sshKeysObjectName,
		)
	}
	return keys, issues, nil
}

func setUserAuthorizedKeysInGCS(
	ctx context.Context, project string, keys AuthorizedKeys,
) (retErr error) {
	bucket, ok := SSHKeysBucketForProject(project)
	if !ok {
		return errors.Newf(
			"GCS SSH key storage is not configured for metadata project %q", project,
		)
	}
	store, err := newSSHKeysObjectStore(ctx)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.CombineErrors(retErr, store.Close()) }()

	if err := store.WriteObject(ctx, bucket, sshKeysObjectName, keys.AsStorageFile()); err != nil {
		return errors.Wrapf(err, "write SSH keys to gs://%s/%s", bucket, sshKeysObjectName)
	}
	return nil
}
