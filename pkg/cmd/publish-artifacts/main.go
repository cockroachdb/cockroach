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
	"flag"
	"fmt"
	"go/build"
	"io"
	"log"
	"mime"
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

var osVersionRe = regexp.MustCompile(`\d+(\.\d+)*-`)

var isRelease = flag.Bool("release", false, "build in release mode instead of bleeding-edge mode")
var destBucket = flag.String("bucket", "", "override default bucket")

func main() {
	flag.Parse()

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
	pkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}
	var versionStr string
	if *isRelease {
		if _, err := version.NewVersion(branch); err != nil {
			log.Fatalf("refusing to build release with invalid version name '%s' (err: %s)", branch, err)
		}
		versionStr = branch
	} else {
		cmd := exec.Command("git", "rev-parse", "HEAD")
		cmd.Dir = pkg.Dir
		log.Printf("%s %s", cmd.Env, cmd.Args)
		out, err := cmd.Output()
		if err != nil {
			log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
		}
		versionStr = string(bytes.TrimSpace(out))
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		log.Fatalf("AWS session: %s", err)
	}
	svc := s3.New(sess)

	var bucketName string
	if len(*destBucket) > 0 {
		bucketName = *destBucket
	} else if *isRelease {
		bucketName = "binaries.cockroachdb.com"
	} else {
		bucketName = "cockroach"
	}
	log.Printf("Using S3 bucket: %s", bucketName)

	// TODO(tamird,benesch,bdarnell): make "latest" a website-redirect
	// rather than a full key. This means that the actual artifact will no
	// longer be named "-latest".
	latestStr := "latest"
	releaseVersionStrs := []string{versionStr, latestStr}

	noCache := "no-cache"

	if *isRelease {
		for _, releaseVersionStr := range releaseVersionStrs {
			archiveBase := fmt.Sprintf("cockroach-%s", releaseVersionStr)
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
			putObjectInput := s3.PutObjectInput{
				Bucket: &bucketName,
				Key:    &srcArchive,
				Body:   f,
			}
			if releaseVersionStr == latestStr {
				putObjectInput.CacheControl = &noCache
			}
			if _, err := svc.PutObject(&putObjectInput); err != nil {
				log.Fatalf("s3 upload %s: %s", absoluteSrcArchivePath, err)
			}
			if err := f.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}

	for _, target := range []struct {
		buildType  string
		baseSuffix string
	}{
		// TODO(tamird): consider shifting this information into the builder
		// image; it's conceivable that we'll want to target multiple versions
		// of a given triple.
		{buildType: "release-darwin", baseSuffix: "darwin-10.9-amd64"},
		{buildType: "release-linux-gnu", baseSuffix: "linux-2.6.32-gnu-amd64"},
		{buildType: "release-linux-musl", baseSuffix: "linux-2.6.32-musl-amd64"},
		{buildType: "release-windows", baseSuffix: "windows-6.2-amd64.exe"},
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
			if i > 0 && (*isRelease || !strings.HasSuffix(target.buildType, "linux-gnu")) {
				continue
			}
			// race doesn't work without glibc on Linux. See
			// https://github.com/golang/go/issues/14481.
			if strings.HasSuffix(target.buildType, "linux-musl") && strings.Contains(extraArgs.goflags, "-race") {
				continue
			}

			{
				recipe := "build"
				// TODO(tamird, #14673): make CCL compile on Windows.
				if strings.HasSuffix(target.buildType, "windows") {
					recipe = "buildoss"
				}
				args := []string{recipe}
				args = append(args, fmt.Sprintf("%s=%s", "TYPE", target.buildType))
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

			if strings.Contains(target.buildType, "linux") {
				binaryName := fmt.Sprintf("./cockroach%s-%s", extraArgs.suffix, target.baseSuffix)

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
				if !strings.HasSuffix(target.buildType, "linux-musl") {
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

			base := fmt.Sprintf("cockroach%s-%s", extraArgs.suffix, target.baseSuffix)

			absolutePath := filepath.Join(pkg.Dir, base)
			binary, err := os.Open(absolutePath)
			if err != nil {
				log.Fatalf("os.Open(%s): %s", absolutePath, err)
			}

			const dotExe = ".exe"
			hasExe := strings.HasSuffix(base, dotExe)

			if !*isRelease {
				const repoName = "cockroach"

				remoteName := strings.TrimSuffix(base, dotExe)
				// Replace cockroach{suffix}-{target suffix} with
				// cockroach{suffix}.{target suffix}.
				remoteName = strings.Replace(remoteName, "-", ".", 1)
				// TODO(tamird): do we want to keep doing this? No longer
				// doing so requires updating cockroachlabs/production, and
				// possibly cockroachdb/cockroach-go.
				remoteName = osVersionRe.ReplaceAllLiteralString(remoteName, "")

				fileName := fmt.Sprintf("%s.%s", remoteName, versionStr)
				if hasExe {
					fileName += dotExe
				}
				disposition := mime.FormatMediaType("attachment", map[string]string{"filename": fileName})

				// NB: The leading slash is required to make redirects work
				// correctly since we reuse this key as the redirect location.
				versionKey := fmt.Sprintf("/%s/%s", repoName, fileName)
				if _, err := svc.PutObject(&s3.PutObjectInput{
					Bucket:             &bucketName,
					ContentDisposition: &disposition,
					Key:                &versionKey,
					Body:               binary,
				}); err != nil {
					log.Fatalf("s3 upload %s: %s", absolutePath, err)
				}
				latestSuffix := branch
				if latestSuffix == "master" {
					latestSuffix = "LATEST"
				}
				latestKey := fmt.Sprintf("%s/%s.%s", repoName, remoteName, latestSuffix)
				if _, err := svc.PutObject(&s3.PutObjectInput{
					Bucket:       &bucketName,
					CacheControl: &noCache,
					Key:          &latestKey,
					WebsiteRedirectLocation: &versionKey,
				}); err != nil {
					log.Fatalf("s3 redirect to %s: %s", versionKey, err)
				}
			} else {
				targetSuffix := strings.TrimSuffix(target.baseSuffix, dotExe)
				// TODO(tamird): remove this weirdness. Requires updating
				// "users" e.g. docs, cockroachdb/cockroach-go, maybe others.
				if strings.Contains(target.buildType, "linux") {
					targetSuffix = strings.Replace(targetSuffix, "gnu-", "", -1)
					targetSuffix = osVersionRe.ReplaceAllLiteralString(targetSuffix, "")
				}

				// Stat the binary. Info is needed for archive headers.
				binaryInfo, err := binary.Stat()
				if err != nil {
					log.Fatal(err)
				}

				for _, releaseVersionStr := range releaseVersionStrs {
					archiveBase := fmt.Sprintf("cockroach-%s", releaseVersionStr)
					targetArchiveBase := fmt.Sprintf("%s.%s", archiveBase, targetSuffix)
					var targetArchive string
					var body bytes.Buffer
					if hasExe {
						targetArchive = targetArchiveBase + ".zip"
						zw := zip.NewWriter(&body)

						// Set the zip header from the file info. Overwrite name.
						zipHeader, err := zip.FileInfoHeader(binaryInfo)
						if err != nil {
							log.Fatal(err)
						}
						zipHeader.Name = filepath.Join(targetArchiveBase, "cockroach.exe")

						zfw, err := zw.CreateHeader(zipHeader)
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
						targetArchive = targetArchiveBase + ".tgz"
						gzw := gzip.NewWriter(&body)
						tw := tar.NewWriter(gzw)

						// Set the tar header from the file info. Overwrite name.
						tarHeader, err := tar.FileInfoHeader(binaryInfo, "")
						if err != nil {
							log.Fatal(err)
						}
						tarHeader.Name = filepath.Join(targetArchiveBase, "cockroach")
						if err := tw.WriteHeader(tarHeader); err != nil {
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
					if _, err := binary.Seek(0, 0); err != nil {
						log.Fatal(err)
					}
					putObjectInput := s3.PutObjectInput{
						Bucket: &bucketName,
						Key:    &targetArchive,
						Body:   bytes.NewReader(body.Bytes()),
					}
					if releaseVersionStr == latestStr {
						putObjectInput.CacheControl = &noCache
					}
					if _, err := svc.PutObject(&putObjectInput); err != nil {
						log.Fatalf("s3 upload %s: %s", targetArchive, err)
					}
				}
			}
			if err := binary.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}
}
