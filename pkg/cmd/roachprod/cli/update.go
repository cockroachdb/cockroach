// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/option"
)

func getBlob(
	client *storage.Client, branch string, os string, arch string, blob string,
) *storage.ObjectHandle {
	return client.Bucket("cockroach-nightly").Object(fmt.Sprintf("binaries/%s/%s/%s/%s", branch, os, arch, blob))
}

func checkLatest(
	client *storage.Client, ctx context.Context, branch string, os string, arch string,
) (time.Time, string, error) {
	shaObj := getBlob(client, branch, os, arch, "latest_sha")

	errMsg := func(err error) error {
		return errors.Wrapf(err, "unable to read blob: %q", shaObj.ObjectName())
	}
	attrs, err := shaObj.Attrs(ctx)
	if err != nil {
		return time.Time{}, "", errMsg(err)
	}
	// Read the latest sha.
	r, err := shaObj.NewReader(ctx)
	if err != nil {
		return time.Time{}, "", errMsg(err)
	}
	defer func() { _ = r.Close() }()
	body, err := io.ReadAll(r)
	if err != nil {
		return time.Time{}, "", errMsg(err)
	}
	return attrs.Updated, strings.TrimSuffix(string(body), "\n"), nil
}

func CheckLatest(branch string, os string, arch string) (time.Time, string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		return time.Time{}, "", err
	}
	return checkLatest(client, ctx, branch, os, arch)
}

// DownloadLatestRoachprod downloads the latest binary into the file specified by `toFile`.
func DownloadLatestRoachprod(toFile string, branch string, osName string, arch string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		return err
	}
	// Grab the latest sha.
	updateTime, sha, err := checkLatest(client, ctx, branch, osName, arch)
	if err != nil {
		return err
	}
	fmt.Println("Found latest sha:", sha)
	// Ensure the current binary is _older_ than the latest.
	age, err := TimeSinceUpdate(updateTime)
	if err != nil {
		return errors.Wrapf(err, "unable to determine 'mtime' of the roachprod binary")
	}
	if age.Minutes() <= 0 {
		return errors.Newf("refusing to update; current binary is newer than the latest: %s", age)
	}
	binObj := getBlob(client, branch, osName, arch, "roachprod."+sha)
	// Get the blob and write it to a file.
	r, err := binObj.NewReader(ctx)
	if err != nil {
		return errors.Wrapf(err, "unable to read blob: %q", binObj.ObjectName())
	}
	defer func() { _ = r.Close() }()

	f, err := os.Create(toFile)
	if err != nil {
		return errors.Wrapf(err, "unable to create destination file: %q", toFile)
	}
	if err := f.Chmod(0700); err != nil {
		return errors.Wrapf(err, "unable to chmod destination file: %q", toFile)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, r); err != nil {
		return errors.Wrapf(err, "unable to copy blob: %q to the destination file: %q", binObj.ObjectName(), toFile)
	}
	return nil
}
