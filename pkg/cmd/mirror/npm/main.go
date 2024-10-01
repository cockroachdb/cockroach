// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	yaml "gopkg.in/yaml.v3"
)

// getUnmirroredUrls finds packages in each provided pnpm-lock.yaml (each of
// yamlLockfilePaths), filters for only ones that haven't already been uploaded
// to a GCS bucket, and returns a list of npmjs.org URLs that need to be
// mirrored.
func getUnmirroredUrls(
	ctx context.Context, bucket *storage.BucketHandle, yamlLockfilePaths []string,
) ([]*url.URL, error) {
	urls := []*url.URL{}

	mirrored, err := listMirroredFiles(ctx, bucket)
	if err != nil {
		return nil, err
	}

	for _, path := range yamlLockfilePaths {
		lf := new(PnpmLockfile)
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("unable to read lockfile %q: %w", path, err)
		}
		if err := yaml.Unmarshal(contents, &lf); err != nil { // nolint:yaml
			return nil, fmt.Errorf("unable to parse contents of %q as YAML: %w", path, err)
		}

		for key := range lf.Packages {
			// Example key:
			// /@ant-design/create-react-context@0.2.6(prop-types@15.8.1)(react@16.12.0)
			//  ~~~~~~~~~~~ ~~~~~~~~~~~~~~~~~~~~ ~~~~~
			//  [NPM scope] package name         version

			// Remove the leading '/' and everything after the first '('.
			scopedNameAtVersion, _, _ := strings.Cut(key[1:], "(")

			// Split the scoped name and version apart.
			lastAt := strings.LastIndex(scopedNameAtVersion, "@")
			if lastAt < 1 {
				// A lastAt of zero means we only found the @ in '@scope/name'
				// of an NPM package. Minus one means no @ was found at all.
				// Either way, that means we haven't found a version.
				return nil, fmt.Errorf("unable to parse package key %q into name and version", key)
			}
			scopedName := scopedNameAtVersion[:lastAt]
			version := scopedNameAtVersion[lastAt+1:]

			// Separate the scope (if present) and name.
			scope, name, pkgIsScoped := strings.Cut(scopedName, "/")
			if !pkgIsScoped {
				// If the package name wasn't scoped (i.e. didn't contain a '/'),
				// 'scope' contains the package's name and 'name' is empty.
				// Swap them to get values in the right places semantically.
				name = scope
				scope = ""
			}

			// Build a registry.npmjs.org URL path to download that package,
			// which corresponds to the package's object name in GCS.
			// Example URL:
			// https://registry.npmjs.org/@ant-design/create-react-context/-/create-react-context-0.2.5.tgz
			//                            ~~~~~~~~~~~ ~~~~~~~~~~~~~~~~~~~~   ~~~~~~~~~~~~~~~~~~~~ ~~~~~
			//                            [NPM scope] package name           package name         version
			pathWithoutLeadingSlash := fmt.Sprintf(
				"%s/-/%s-%s.tgz",
				name,
				name,
				version,
			)
			// Packages without a scope don't have the first path segment
			// ('@ant-design/' in the above example), so only add it for scoped
			// packages.
			if pkgIsScoped {
				pathWithoutLeadingSlash = fmt.Sprintf("%s/%s", scope, pathWithoutLeadingSlash)
			}

			// Check for a GCS object named after that path.
			_, alreadyMirrored := mirrored[pathWithoutLeadingSlash]
			if alreadyMirrored {
				// Ignore packages that have already been mirrored to our GCS bucket.
				// Since pnpm doesn't include URLs in lockfiles, there's no need to
				// update pnpm-lock.yaml.
				continue
			}

			// Generate a URL at which that package can be downloaded for later uploads.
			npmUrl, err := url.Parse("https://registry.npmjs.org/" + pathWithoutLeadingSlash)
			if err != nil {
				return nil, fmt.Errorf("couldn't generate valid *url.URL for registry.npmjs.org: %w", err)
			}
			urls = append(urls, npmUrl)
		}
	}

	return urls, nil
}

// listMirroredFiles returns a map of object names to object{}, containing one
// entry for each object in the provided GCS bucket.
func listMirroredFiles(
	ctx context.Context, bucket *storage.BucketHandle,
) (map[string]struct{}, error) {
	objIterator := bucket.Objects(ctx, &storage.Query{
		Projection: storage.ProjectionNoACL,
		Versions:   false,
	})

	names := map[string]struct{}{}
	for {
		attrs, err := objIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("unexpected error when listing objects: %+w", err)
		}
		names[attrs.Name] = struct{}{}
	}
	return names, nil
}

// mirrorDependencies downloads all files in public registries listed in the
// provided URLs and reuploads them to a Google Cloud Storage bucket controlled
// by Cockroach Labs.
func mirrorDependencies(
	ctx context.Context, bucket *storage.BucketHandle, publicPackageUrls []*url.URL,
) error {
	// Dedupe dependencies between lockfiles prevent multiple Goroutines from
	// writing the same file, and to minimize work (multiple projects depend on the
	// same version of typescript, for example). This is unlikely to have many collisions
	// thanks to pnpm's support for a top-level, workspace-wide lockfile, but is still
	// safe to do.
	urls := map[*url.URL]struct{}{}
	for _, u := range publicPackageUrls {
		_, alreadySeen := urls[u]
		if alreadySeen {
			continue
		}

		urls[u] = struct{}{}
	}

	// Download and upload files in parallel.
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.GOMAXPROCS(0))
	for url := range urls {
		url := url
		fmt.Fprintf(os.Stderr, "DEBUG: Mirroring %s...\n", url)
		g.Go(func() error {
			return mirrorUrl(ctx, bucket, url)
		})
	}
	return g.Wait()
}

// mirrorUrl downloads the single file at url and reuploads it to bucket.
func mirrorUrl(ctx context.Context, bucket *storage.BucketHandle, url *url.URL) error {
	// Download the file.
	tgzUrl := url.String()
	res, err := http.DefaultClient.Get(tgzUrl)
	if err != nil {
		return fmt.Errorf("unable to request file %q: %w", tgzUrl, err)
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
		return fmt.Errorf("unexpected error while uploading %q: %w", url.Path, err)
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

func main() {
	shouldMirror := flag.Bool("mirror", false, "Download and reupload unmirrored dependencies")
	flag.Parse()
	lockfiles := flag.Args()
	if len(lockfiles) == 0 {
		fmt.Println("ERROR: no lockfiles provided at CLI")
		os.Exit(1)
	}
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Println("ERROR: unable to create Google Cloud Storage client:", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	const mirrorBucketName = "cockroach-npm-deps"
	bucket := client.Bucket(mirrorBucketName)

	toMirror, err := getUnmirroredUrls(ctx, bucket, lockfiles)
	if err != nil {
		fmt.Println("ERROR: ", err)
		os.Exit(1)
	}

	if len(toMirror) == 0 {
		fmt.Fprintf(os.Stderr, "INFO: no dependencies to mirror!\n")
		os.Exit(0)
	}

	if !*shouldMirror {
		fmt.Println(
			strings.TrimSpace(`
Some dependencies have not been mirrored to Google Cloud Storage, and will not
be available to Bazel builds until they are mirrored:`,
			) + "\n",
		)
		for _, dep := range toMirror {
			fmt.Println(dep.String())
		}
		fmt.Println("\nRun 'dev ui mirror-deps' to mirror them.")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "INFO: mirroring %d dependencies to GCS\n", len(toMirror))
	if err := mirrorDependencies(ctx, bucket, toMirror); err != nil {
		fmt.Println("ERROR: ", err)
		os.Exit(1)
	}
}
