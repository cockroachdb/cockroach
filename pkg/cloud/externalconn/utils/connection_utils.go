// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package utils

import (
	"bytes"
	"context"
	"io/ioutil"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
)

const markerFile = "crdb_external_storage_location"

// CheckExternalStorageConnection writes a sentinel file, lists the file, and reads the file
// back. This serves as a sanity check that the external connection represents
// an ExternalStorage resource that can be connected and interacted with..
func CheckExternalStorageConnection(
	ctx context.Context, execCfg interface{}, user username.SQLUsername, uri string,
) error {
	cfg := execCfg.(*sql.ExecutorConfig)
	es, err := cfg.DistSQLSrv.ExternalStorageFromURI(ctx, uri, user)
	if err != nil {
		return err
	}
	defer es.Close()

	if cfg.ExternalConnectionTestingKnobs != nil &&
		cfg.ExternalConnectionTestingKnobs.SkipCheckingExternalStorageConnection != nil {
		if cfg.ExternalConnectionTestingKnobs.SkipCheckingExternalStorageConnection() {
			return nil
		}
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
	reader, err := es.ReadFile(ctx, markerFile)
	if err != nil {
		return errors.Wrap(err, "failed to read sentinel ExternalStorage file")
	}

	content, err := ioutil.ReadAll(ioctx.ReaderCtxAdapter(ctx, reader))
	if err != nil {
		return errors.Wrap(err, "failed to read sentinel ExternalStorage file content")
	}
	if markerContent != string(content) {
		return errors.Newf("content mismatch, expected: %s but found: %s", markerContent, string(content))
	}

	return nil
}
