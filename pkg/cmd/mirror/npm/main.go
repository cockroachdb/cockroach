// Copyright 2022 The Cockroach Authors.
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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
)

// parseLockfiles reads .json files from-disk and JSON-deserializes them into a
// map of file path to array of LockfileEntry structs.
func parseLockfiles(jsonLockfiles []string) (Lockfiles, error) {
	entries := Lockfiles{}
	for _, path := range jsonLockfiles {
		lf := new(Lockfile)
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("unable to read lockfile %q: %v", path, err)
		}
		if err := json.Unmarshal(contents, &lf); err != nil {
			return nil, fmt.Errorf("unable to parse contents of %q as JSON: %v", path, err)
		}

		pathEntries := []LockfileEntry{}
		for name, entry := range *lf {
			entry.Name = name
			pathEntries = append(pathEntries, entry)
		}
		entries[path] = pathEntries
	}

	return entries, nil
}

const mirrorBucketName = "cockroach-npm-deps"

// mirrorDependencies downloads all files in public registries listed in the
// provided lockfiles and reuploads them to a Google Cloud Storage bucket
// controlled by Cockroach Labs.
func mirrorDependencies(ctx context.Context, lockfiles Lockfiles) error {
	// Dedupe dependencies between lockfiles prevent multiple Goroutines from
	// writing the same file, and to minimize work (multiple projects depend on the
	// same version typescript, for example).
	pathsToUrls := map[string]*url.URL{}
	for _, entries := range lockfiles {
		for _, entry := range entries {
			if entry.Resolved == nil {
				continue
			}
			if _, exists := pathsToUrls[entry.Resolved.Path]; exists {
				continue
			}

			pathsToUrls[entry.Resolved.Path] = entry.Resolved
		}
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("unable to create Google Cloud Storage client: %v", err)
	}
	bucket := client.Bucket(mirrorBucketName)

	// Download and upload files in parallel.
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.GOMAXPROCS(0))
	for _, url := range pathsToUrls {
		url := url
		g.Go(func() error {
			return mirrorUrl(ctx, bucket, url)
		})

	}
	return g.Wait()
}

// mirrorUrl downloads the single file at url and reuploads it to bucket.
func mirrorUrl(ctx context.Context, bucket *storage.BucketHandle, url *url.URL) error {
	const yarnRegistry = "registry.yarnpkg.com"
	const npmjsComRegistry = "registry.npmjs.com"
	const npmjsOrgRegistry = "registry.npmjs.org"

	if url == nil {
		return nil
	}

	// Only mirror known registries.
	hostname := url.Hostname()
	if hostname != yarnRegistry && hostname != npmjsComRegistry && hostname != npmjsOrgRegistry {
		return nil
	}

	// Download the file.
	tgzUrl := url.String()
	res, err := http.DefaultClient.Get(tgzUrl)
	if err != nil {
		return fmt.Errorf("unable to request file %q: %v", tgzUrl, err)
	}

	// Bail if the file couldn't be downloaded.
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		defer res.Body.Close()
		return fmt.Errorf("received non-200 status code %q from %q. body = %s", res.Status, tgzUrl, body)
	}
	defer res.Body.Close()

	// Removing the leading /, so that files don't have to be downloaded from
	// storage.googleapis.com/bucket-name//foo (note the extra slash).
	uploadPath, err := filepath.Rel("/", url.Path)
	if err != nil {
		return fmt.Errorf("could not relativize path %q", url.Path)
	}

	// Then upload the file to GCS.
	upload := bucket.Object(uploadPath).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	if _, err := io.Copy(upload, res.Body); err != nil {
		return fmt.Errorf("unexpected error while uploading %q: %v", url.Path, err)
	}

	// Support failures of the DoesNotExist precondition, to avoid another round-trip to GCS.
	if err := upload.Close(); err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusPreconditionFailed {
				// In this case the "DoesNotExist" precondition
				// failed, i.e., the object does already exist.
				return nil
			}
			return gerr
		}
		return err
	}
	return nil
}

// updateLockfileUrls rewrites the URL for every entry in each provided lockfile
// to point to the same file hosted by the GCS bucket Cockroach Labs controls,
// skipping URLs that already point to that bucket.
func updateLockfileUrls(ctx context.Context, lockfiles Lockfiles) error {
	for _, entries := range lockfiles {
		for _, entry := range entries {
			if entry.Resolved == nil || entry.Resolved.Host == "storage.googleapis.com" {
				continue
			}
			newPath, err := url.JoinPath(mirrorBucketName, entry.Resolved.Path)
			if err != nil {
				return fmt.Errorf("unable to rewrite URL %q: %v", entry.Resolved.String(), err)
			}
			entry.Resolved.Path = newPath
			entry.Resolved.Host = "storage.googleapis.com"
		}
	}
	return nil
}

// writeNewLockfileJsons re-serializes a set of lockfile models, saving them to
// the local filesystem next to their original versions with ".new" added to
// the filename (e.g. the model for /path/to/foo.yarn.json is written to
// /path/to/foo.yarn.json.new).
func writeNewLockfileJsons(ctx context.Context, lockfiles Lockfiles) error {
	for filename, entries := range lockfiles {
		out := Lockfile{}
		for _, entry := range entries {
			out[entry.Name] = entry
		}
		asJson, err := json.Marshal(out)
		if err != nil {
			return fmt.Errorf("unable to marshal new lockfile to JSON: %v", err)
		}
		outname := filename + ".new"
		if err := os.WriteFile(outname, asJson, os.ModePerm); err != nil {
			return fmt.Errorf("unable to write new file %q: %v", outname, err)
		}
	}

	return nil
}

func main() {
	var shouldMirror = flag.Bool("mirror", false, "mirrors dependencies to GCS instead of regenerate yarn.lock files.")
	flag.Parse()

	lockfiles, err := parseLockfiles(flag.Args())
	if err != nil {
		fmt.Println("ERROR: ", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if shouldMirror != nil && *shouldMirror {
		fmt.Fprintln(os.Stderr, "INFO: mirroring dependencies to GCS")
		if err := mirrorDependencies(ctx, lockfiles); err != nil {
			fmt.Println("ERROR: ", err)
			os.Exit(1)
		}
	} else {
		fmt.Fprintln(os.Stderr, "INFO: regenerating *.yarn.json files with GCS URLs")
		if err := updateLockfileUrls(ctx, lockfiles); err != nil {
			fmt.Println("ERROR: ", err)
			os.Exit(1)
		}

		if err := writeNewLockfileJsons(ctx, lockfiles); err != nil {
			fmt.Println("ERROR: ", err)
			os.Exit(1)
		}
	}
}
