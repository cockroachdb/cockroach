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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"go/build"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	version "github.com/hashicorp/go-version"
)

const (
	awsAccessKeyIDKey      = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyKey  = "AWS_SECRET_ACCESS_KEY"
	teamcityBuildBranchKey = "TC_BUILD_BRANCH"
)

var libsRe = func() *regexp.Regexp {
	libs := strings.Join([]string{
		regexp.QuoteMeta("linux-vdso.so."),
		regexp.QuoteMeta("librt.so."),
		regexp.QuoteMeta("libpthread.so."),
		regexp.QuoteMeta("libm.so."),
		regexp.QuoteMeta("libc.so."),
		strings.Replace(regexp.QuoteMeta("ld-linux-ARCH.so."), "ARCH", ".*", -1),
	}, "|")
	return regexp.MustCompile(libs)
}()

func main() {
	if _, ok := os.LookupEnv(awsAccessKeyIDKey); !ok {
		log.Fatalf("AWS access key ID environment variable %s is not set", awsAccessKeyIDKey)
	}
	if _, ok := os.LookupEnv(awsSecretAccessKeyKey); !ok {
		log.Fatalf("AWS secret access key environment variable %s is not set", awsSecretAccessKeyKey)
	}

	branch, ok := os.LookupEnv(teamcityBuildBranchKey)
	if !ok {
		log.Fatalf("VCS branch environment variable %s is not set", teamcityBuildBranchKey)
	}
	isHead := branch == "master"
	pkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}
	var versionStr string
	switch {
	case isHead:
		cmd := exec.Command("git", "rev-parse", "HEAD")
		cmd.Dir = pkg.Dir
		log.Printf("%s %s", cmd.Env, cmd.Args)
		out, err := cmd.Output()
		if err != nil {
			log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
		}
		versionStr = string(bytes.TrimSpace(out))
	case strings.HasPrefix(branch, "beta-"):
		versionStr = branch
	default:
		if _, err := version.NewVersion(branch); err != nil {
			log.Fatal(err)
		}
		versionStr = branch
	}

	for _, buildType := range []string{
		"release-darwin",
		"release-linux-gnu",
		"release-linux-musl",
		"release-windows",
	} {
		for i, extraArgs := range []struct {
			goflags string
			suffix  string
			tags    string
		}{
			{},
			// TODO(tamird): re-enable deadlock builds. This really wants its
			// own install suffix; it currently pollutes the normal release
			// build cache.
			//
			// {suffix: ".deadlock", tags: "deadlock"},
			{suffix: ".race", goflags: "-race"},
		} {
			// TODO(tamird): build deadlock,race binaries for all targets?
			if i > 0 && !strings.HasSuffix(buildType, "linux-gnu") {
				continue
			}
			// race doesn't work without glibc on Linux. See
			// https://github.com/golang/go/issues/14481.
			if strings.HasSuffix(buildType, "linux-musl") && strings.Contains(extraArgs.goflags, "-race") {
				continue
			}

			{
				target := "build"
				// TODO(tamird, #14673): make CCL compile on Windows.
				if strings.HasSuffix(buildType, "windows") {
					target = "buildoss"
				}
				args := []string{target}
				args = append(args, fmt.Sprintf("%s=%s", "TYPE", buildType))
				args = append(args, fmt.Sprintf("%s=%s", "GOFLAGS", extraArgs.goflags))
				args = append(args, fmt.Sprintf("%s=%s", "SUFFIX", extraArgs.suffix))
				args = append(args, fmt.Sprintf("%s=%s", "TAGS", extraArgs.tags))
				cmd := exec.Command("make", args...)
				cmd.Dir = pkg.Dir
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				log.Printf("%s %s", cmd.Env, cmd.Args)
				if err := cmd.Run(); err != nil {
					log.Fatalf("%s: %s", cmd.Args, err)
				}
			}

			if strings.Contains(buildType, "linux") {
				binaryName := fmt.Sprintf("./cockroach%s-linux-2.6.32-gnu-amd64", extraArgs.suffix)

				cmd := exec.Command(binaryName, "version")
				cmd.Dir = pkg.Dir
				cmd.Env = append(cmd.Env, "MALLOC_CONF=prof:true")
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				log.Printf("%s %s", cmd.Env, cmd.Args)
				if err := cmd.Run(); err != nil {
					log.Fatalf("%s %s: %s", cmd.Env, cmd.Args, err)
				}

				// ldd only works on binaries built for the host. "linux-musl"
				// produces fully static binaries, which cause ldd to exit
				// non-zero.
				//
				// TODO(tamird): implement this for all targets.
				if !strings.HasSuffix(buildType, "linux-musl") {
					cmd := exec.Command("ldd", binaryName)
					cmd.Dir = pkg.Dir
					log.Printf("%s %s", cmd.Env, cmd.Args)
					out, err := cmd.Output()
					if err != nil {
						log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
					}
					scanner := bufio.NewScanner(bytes.NewReader(out))
					for scanner.Scan() {
						if line := scanner.Text(); !libsRe.MatchString(line) {
							log.Fatalf("%s is not properly statically linked:\n%s", binaryName, out)
						}
					}
					if err := scanner.Err(); err != nil {
						log.Fatal(err)
					}
				}
			}
		}
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		log.Fatalf("AWS session: %s", err)
	}
	svc := s3.New(sess)

	if isHead {
		// Can't be const because we take its address.
		bucketName := "cockroach"

		for relativePath, remoteName := range map[string]string{
			"cockroach-darwin-10.9-amd64":       "cockroach.darwin-amd64",
			"cockroach-linux-2.6.32-gnu-amd64":  "cockroach.linux-gnu-amd64",
			"cockroach-linux-2.6.32-musl-amd64": "cockroach.linux-musl-amd64",
			"cockroach-windows-6.2-amd64.exe":   "cockroach.windows-amd64.exe",
		} {
			const repoName = "cockroach"

			absolutePath := filepath.Join(pkg.Dir, relativePath)
			f, err := os.Open(absolutePath)
			if err != nil {
				log.Fatalf("os.Open(%s): %s", absolutePath, err)
			}
			// NB: This slash is required to make redirects work correctly
			// since we reuse this key as the redirect location.
			versionKey := fmt.Sprintf("/%s/%s.%s", repoName, remoteName, versionStr)
			if _, err := svc.PutObject(&s3.PutObjectInput{
				Bucket: &bucketName,
				Key:    &versionKey,
				Body:   f,
			}); err != nil {
				log.Fatalf("s3 upload %s: %s", absolutePath, err)
			}
			if err := f.Close(); err != nil {
				log.Fatal(err)
			}
			latestKey := fmt.Sprintf("%s/%s.%s", repoName, remoteName, "LATEST")
			if _, err := svc.PutObject(&s3.PutObjectInput{
				Bucket: &bucketName,
				Key:    &latestKey,
				WebsiteRedirectLocation: &versionKey,
			}); err != nil {
				log.Fatalf("s3 redirect to %s: %s", versionKey, err)
			}
		}
	} else {
		// Can't be const because we take its address.
		bucketName := "binaries.cockroachdb.com"

		versionStrs := []string{versionStr, "latest"}

		// TODO(tamird,benesch,bdarnell): make "latest" a website-redirect
		// rather than a full key. This means that the actual artifact will no
		// longer be named "-latest".
		for _, archiveSuffix := range versionStrs {
			archiveBase := fmt.Sprintf("cockroach-%s", archiveSuffix)
			srcArchive := fmt.Sprintf("%s.%s", archiveBase, "src.tgz")
			cmd := exec.Command(
				"make",
				"archive",
				fmt.Sprintf("ARCHIVE_BASE=%s", archiveBase),
				fmt.Sprintf("ARCHIVE=%s", srcArchive),
			)
			cmd.Dir = pkg.Dir
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			log.Printf("%s %s", cmd.Env, cmd.Args)
			if err := cmd.Run(); err != nil {
				log.Fatalf("%s: %s", cmd.Args, err)
			}

			absoluteSrcArchivePath := filepath.Join(pkg.Dir, srcArchive)
			f, err := os.Open(absoluteSrcArchivePath)
			if err != nil {
				log.Fatalf("os.Open(%s): %s", absoluteSrcArchivePath, err)
			}
			if _, err := svc.PutObject(&s3.PutObjectInput{
				Bucket: &bucketName,
				Key:    &srcArchive,
				Body:   f,
			}); err != nil {
				log.Fatalf("s3 upload %s: %s", absoluteSrcArchivePath, err)
			}
			if err := f.Close(); err != nil {
				log.Fatal(err)
			}

			for relativePath, targetSuffix := range map[string]string{
				"cockroach-darwin-10.9-amd64": "darwin-10.9-amd64",
				// TODO(tamird): make this linux-gnu-amd64 and add versions to
				// both Linux targets. Requires updating "users" e.g. docs,
				// https://godoc.org/github.com/cockroachdb/cockroach-
				// go/testserver#NewTestServer, maybe others.
				"cockroach-linux-2.6.32-gnu-amd64":  "linux-amd64",
				"cockroach-linux-2.6.32-musl-amd64": "linux-musl-amd64",
				"cockroach-windows-6.2-amd64.exe":   "windows-6.2-amd64",
			} {
				absolutePath := filepath.Join(pkg.Dir, relativePath)
				binary, err := os.Open(absolutePath)
				if err != nil {
					log.Fatalf("os.Open(%s): %s", absolutePath, err)
				}
				targetArchiveBase := fmt.Sprintf("%s.%s", archiveBase, targetSuffix)
				var f *os.File
				if strings.HasSuffix(relativePath, ".exe") {
					absoluteArchivePath := filepath.Join(pkg.Dir, targetArchiveBase+".zip")
					f, err = os.Create(absoluteArchivePath)
					if err != nil {
						log.Fatalf("os.Create(%s): %s", absoluteArchivePath, err)
					}
					zw := zip.NewWriter(f)
					zfw, err := zw.Create(filepath.Join(targetArchiveBase, "cockroach.exe"))
					if err != nil {
						log.Fatal(err)
					}
					if _, err := io.Copy(zfw, binary); err != nil {
						log.Fatal(err)
					}
					if err := zw.Close(); err != nil {
						log.Fatal(err)
					}
				} else {
					absoluteArchivePath := filepath.Join(pkg.Dir, targetArchiveBase+".tgz")
					f, err = os.Create(absoluteArchivePath)
					if err != nil {
						log.Fatalf("os.Create(%s): %s", absoluteArchivePath, err)
					}
					gzw := gzip.NewWriter(f)
					tw := tar.NewWriter(gzw)
					binaryInfo, err := binary.Stat()
					if err != nil {
						log.Fatal(err)
					}
					if err := tw.WriteHeader(&tar.Header{
						Name: filepath.Join(targetArchiveBase, "cockroach"),
						Mode: 0755,
						Size: binaryInfo.Size(),
					}); err != nil {
						log.Fatal(err)
					}
					if _, err := io.Copy(tw, binary); err != nil {
						log.Fatal(err)
					}
					if err := tw.Close(); err != nil {
						log.Fatal(err)
					}
					if err := gzw.Close(); err != nil {
						log.Fatal(err)
					}
				}
				if err := binary.Close(); err != nil {
					log.Fatal(err)
				}
				if _, err := f.Seek(0, 0); err != nil {
					log.Fatal(err)
				}
				if _, err := svc.PutObject(&s3.PutObjectInput{
					Bucket: &bucketName,
					Key:    &targetArchiveBase,
					Body:   f,
				}); err != nil {
					log.Fatalf("s3 upload %s: %s", f.Name(), err)
				}
				if err := f.Close(); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}
