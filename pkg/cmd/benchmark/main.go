// Copyright 2017 The Cockroach Authors.
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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"go/build"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"golang.org/x/perf/storage"
)

func main() {
	if err := do(context.Background()); err != nil {
		log.Fatal(err)
	}
}

const storageURL = "https://perfdata.golang.org"

const numIterations = 5

const crdb = "github.com/cockroachdb/cockroach"

const serviceAccountJSONEnv = "SERVICE_ACCOUNT_JSON"

func do(ctx context.Context) error {
	rootPkg, err := build.Import(crdb, "", 0)
	if err != nil {
		return errors.Wrap(err, "could not find repository root")
	}

	show, err := func() ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", "show", "-s", "--format=%H%n%cI", "HEAD")
		cmd.Dir = rootPkg.Dir
		return cmd.Output()
	}()
	if err != nil {
		return errors.Wrap(err, "could not determine commit hash and committer date")
	}
	parts := strings.Split(string(bytes.TrimSpace(show)), "\n")
	if len(parts) != 2 {
		return errors.Errorf("expected commit hash and committer date, got %s", show)
	}
	commit, commitTime := parts[0], parts[1]

	data, ok := os.LookupEnv(serviceAccountJSONEnv)
	if !ok {
		return errors.Errorf("service account JSON environment variable %s is not set", serviceAccountJSONEnv)
	}

	conf, err := google.JWTConfigFromJSON([]byte(data), "https://www.googleapis.com/auth/userinfo.email")
	if err != nil {
		return errors.Wrap(err, "could not read service account JSON")
	}

	var buffers []bytes.Buffer
	if len(os.Args) > 1 {
		var outBuf, errBuf bytes.Buffer
		cmd := exec.CommandContext(ctx, os.Args[0], os.Args[1:]...)
		cmd.Stdout = &outBuf
		cmd.Stderr = &errBuf
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "could not run %s: %s", cmd.Args, errBuf.String())
		}
		buffers = append(buffers, outBuf)
	} else {
		var err error
		buffers, err = doBenchmarks(ctx, rootPkg)
		if err != nil {
			return err
		}
	}

	client := storage.Client{
		BaseURL:    storageURL,
		HTTPClient: conf.Client(ctx),
	}

	u := client.NewUpload(ctx)

	if err := func() error {
		for i := range buffers {
			buffer := &buffers[i]

			w, err := u.CreateFile(fmt.Sprintf("bench%02d.txt", i))
			if err != nil {
				return errors.Wrap(err, "could not create upload file")
			}

			if _, err := fmt.Fprintf(
				w,
				"commit: %s\ncommit-time: %s\nbranch: %s\niteration: %d\nstart-time: %s\n",
				commit,
				commitTime,
				"master",
				i,
				timeutil.Now().UTC().Format(time.RFC3339),
			); err != nil {
				return errors.Wrapf(err, "could not write header on iteration %d", i)
			}

			if _, err := io.Copy(w, buffer); err != nil {
				return errors.Wrapf(err, "could not write to upload file")
			}
		}

		return nil
	}(); err != nil {
		if err := u.Abort(); err != nil {
			log.Printf("could not abort upload: %s", err)
		}
		return errors.Wrap(err, "could not upload file")
	}

	if _, err := u.Commit(); err != nil {
		return errors.Wrap(err, "could not commit upload")
	}

	return nil
}

func doBenchmarks(ctx context.Context, rootPkg *build.Package) ([]bytes.Buffer, error) {
	findArgs := []string{".", "-type", "f", "-name", "*.test"}

	// Find and delete the per-package *.test binaries; we'll be rebuilding
	// them below.
	{
		cmd := exec.CommandContext(ctx, "find", append(findArgs, "-delete")...)
		cmd.Dir = rootPkg.Dir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return nil, errors.Wrap(err, "could not remove old test binaries")
		}
	}

	// Rebuild the per-package *.test binaries.
	{
		typ := "release-" + runtime.GOOS
		if strings.HasSuffix(typ, "linux") {
			typ += "-gnu"
		}

		cmd := exec.CommandContext(ctx, "make", "-s", "testbuild", "TYPE="+typ)
		cmd.Dir = rootPkg.Dir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return nil, errors.Wrap(err, "could not build test binaries")
		}
	}

	// Find the per-package *.test binaries we built.
	var binaries []string
	{
		cmd := exec.CommandContext(ctx, "find", findArgs...)
		cmd.Dir = rootPkg.Dir
		out, err := cmd.Output()
		if err != nil {
			return nil, errors.Wrapf(err, "could not find new test binaries: %s", out)
		}
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for scanner.Scan() {
			binaries = append(binaries, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return nil, errors.Wrap(err, "could not find new test binaries")
		}
	}

	buffers := make([]bytes.Buffer, numIterations)
	for i := range buffers {
		buffer := &buffers[i]

		for _, txt := range binaries {
			binaryPath, err := filepath.Abs(txt)
			if err != nil {
				return nil, errors.Wrapf(err, "could not find test binary %s on iteration %d", txt, i)
			}

			{
				relativePath, err := filepath.Rel(rootPkg.Dir, binaryPath)
				if err != nil {
					return nil, errors.Wrapf(err, "could not get relative path to binary %s on iteration %d", binaryPath, i)
				}
				pkgName := path.Join(crdb, filepath.Dir(relativePath))

				if _, err := fmt.Fprintf(buffer, "pkg: %s\n", pkgName); err != nil {
					return nil, errors.Wrapf(err, "could not write package name %s on iteration %d", pkgName, i)
				}
			}

			cmd := exec.CommandContext(ctx, binaryPath, "-test.timeout", "0", "-test.run", "-", "-test.bench", ".", "-test.benchmem")
			cmd.Dir = filepath.Dir(binaryPath)
			cmd.Stdout = buffer
			cmd.Stderr = os.Stderr

			log.Printf("iteration %d running: %s", i, cmd.Args)

			if err := cmd.Run(); err != nil {
				return nil, errors.Wrapf(err, "could not run test binary %s on iteration %d", binaryPath, i)
			}
		}
	}

	return buffers, nil
}
