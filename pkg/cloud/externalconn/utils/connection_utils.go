// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

import (
	"bytes"
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const markerFile = "crdb_external_storage_location"

// CheckExternalStorageConnection writes a sentinel file, lists the file, and reads the file
// back. This serves as a sanity check that the external connection represents
// an ExternalStorage resource that can be connected and interacted with.
func CheckExternalStorageConnection(
	ctx context.Context, env externalconn.ExternalConnEnv, uri string,
) error {
	es, err := env.ExternalStorageFromURIFactory(ctx, uri, env.Username)
	if err != nil {
		return err
	}
	defer func() {
		if err := es.Close(); err != nil {
			log.Dev.Warningf(ctx, "failed to close External Storage %+v", err)
		}
	}()

	if env.SkipCheckingExternalStorageConnection {
		return nil
	}

	// Write a sentinel file.
	markerContent := "a CockroachDB cluster has been configured to read and write to this location"
	if err := cloud.WriteFile(ctx, es, markerFile, bytes.NewReader([]byte(markerContent))); err != nil {
		return errors.Wrap(err, "failed to write a sentinel ExternalStorage file")
	}

	// List the sentinel file, using it as the listing prefix so we don't
	// enumerate every object under the URI. List strips the listing prefix from
	// returned names, so the marker itself comes back as "".
	var foundFile bool
	if err := es.List(ctx, markerFile, "", func(s string) error {
		if s == "" {
			foundFile = true
			return cloud.ErrListingDone
		}
		return nil
	}); err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return errors.Wrap(err, "failed to list sentinel ExternalStorage file")
	} else if !foundFile {
		return errors.Newf("failed to find sentinel ExternalStorage file '%s'", markerFile)
	}

	// Read the sentinel file.
	reader, _, err := es.ReadFile(ctx, markerFile, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return errors.Wrap(err, "failed to read sentinel ExternalStorage file")
	}

	content, err := io.ReadAll(ioctx.ReaderCtxAdapter(ctx, reader))
	if err != nil {
		return errors.Wrap(err, "failed to read sentinel ExternalStorage file content")
	}
	if markerContent != string(content) {
		return errors.Newf("content mismatch, expected: %s but found: %s", markerContent, string(content))
	}

	return nil
}

// CheckKMSConnection encrypts, decrypts and matches the contents of a sentinel
// file. This serves as a sanity check that the external connection represents a
// KMS resource that can be connected and interacted with.
func CheckKMSConnection(ctx context.Context, env externalconn.ExternalConnEnv, uri string) error {
	kms, err := cloud.KMSFromURI(ctx, uri, &env)
	if err != nil {
		return err
	}
	defer func() {
		if err := kms.Close(); err != nil {
			log.Dev.Warningf(ctx, "failed to close KMS %+v", err)
		}
	}()

	if env.SkipCheckingKMSConnection {
		return nil
	}

	// Encrypt and decrypt a sentinel file.
	markerContent := "a CockroachDB cluster has been configured to interact with this KMS"
	encryptedContent, err := kms.Encrypt(ctx, []byte(markerContent))
	if err != nil {
		return errors.Wrap(err, "failed to encrypt marker content when setting up External Connection")
	}

	decryptedContent, err := kms.Decrypt(ctx, encryptedContent)
	if err != nil {
		return errors.Wrap(err, "failed to decrypt marker content when setting up External Connection")
	}

	if !bytes.Equal(decryptedContent, []byte(markerContent)) {
		return errors.Newf("content mismatch, expected: %s but found: %s", markerContent, string(decryptedContent))
	}

	return nil
}
