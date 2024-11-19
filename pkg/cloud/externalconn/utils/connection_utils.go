// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

import (
	"bytes"
	"context"
	"io"
	"strings"

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
			log.Warningf(ctx, "failed to close External Storage %+v", err)
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

	// List the sentinel file.
	var foundFile bool
	if err := es.List(ctx, "", "", func(s string) error {
		paths := strings.Split(s, "/")
		s = paths[len(paths)-1]
		if match := strings.HasPrefix(s, markerFile); match {
			foundFile = true
		}
		return err
	}); err != nil {
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
			log.Warningf(ctx, "failed to close KMS %+v", err)
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
