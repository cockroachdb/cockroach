// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"golang.org/x/perf/storage"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func main() {
	if err := do(context.Background()); err != nil {
		log.Fatalf("%+v", err)
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

	rev, err := func() ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
		cmd.Dir = rootPkg.Dir
		return cmd.Output()
	}()
	if err != nil {
		return errors.Wrap(err, "could not determine commit")
	}

	data, ok := os.LookupEnv(serviceAccountJSONEnv)
	if !ok {
		return errors.Errorf("Service account JSON environment variable %s is not set", serviceAccountJSONEnv)
	}

	conf, err := google.JWTConfigFromJSON([]byte(data), "https://www.googleapis.com/auth/userinfo.email")
	if err != nil {
		return errors.Wrap(err, "could not read service account JSON")
	}

	findArgs := []string{".", "-type", "f", "-name", "*.test"}

	// Find and delete the per-package *.test binaries; we'll be rebuilding
	// them below.
	{
		cmd := exec.CommandContext(ctx, "find", append(findArgs, "-delete")...)
		cmd.Dir = rootPkg.Dir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return errors.Wrap(err, "could not remove old test binaries")
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
			return errors.Wrap(err, "could not build test binaries")
		}
	}

	// Find the per-package *.test binaries we built.
	var binaries []string
	{
		cmd := exec.CommandContext(ctx, "find", findArgs...)
		cmd.Dir = rootPkg.Dir
		out, err := cmd.Output()
		if err != nil {
			return errors.Wrapf(err, "could not find new test binaries: %s", out)
		}
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for scanner.Scan() {
			binaries = append(binaries, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return errors.Wrap(err, "could not find new test binaries")
		}
	}

	client := storage.Client{
		BaseURL:    storageURL,
		HTTPClient: conf.Client(ctx),
	}

	u := client.NewUpload(ctx)

	if err := func() error {
		for i := 0; i < numIterations; i++ {
			w, err := u.CreateFile(fmt.Sprintf("bench%02d.txt", i))
			if err != nil {
				return errors.Wrap(err, "could not create file")
			}

			if _, err := fmt.Fprintf(
				w,
				"commit: %s\niteration: %d\nstart-time: %s\n",
				bytes.TrimSpace(rev),
				i,
				timeutil.Now().UTC().Format(time.RFC3339),
			); err != nil {
				return errors.Wrap(err, "could not write header")
			}

			for _, txt := range binaries {
				binaryPath, err := filepath.Abs(txt)
				if err != nil {
					return errors.Wrapf(err, "could not find test binary %s", txt)
				}

				cmd := exec.CommandContext(ctx, binaryPath, "-test.run", "-", "-test.bench", ".", "-test.benchmem")
				cmd.Dir = filepath.Dir(binaryPath)
				cmd.Stdout = io.MultiWriter(w, os.Stdout)
				cmd.Stderr = os.Stderr

				log.Printf("exec: %s", cmd.Args)

				if err := cmd.Run(); err != nil {
					return errors.Wrapf(err, "could not run test binary %q", binaryPath)
				}
			}
		}

		return nil
	}(); err != nil {
		if err := u.Abort(); err != nil {
			log.Println(errors.Wrap(err, "could not abort upload"))
		}
		return errors.Wrap(err, "could not upload file")
	}

	if _, err := u.Commit(); err != nil {
		return errors.Wrap(err, "could not commit upload")
	}

	return nil
}
