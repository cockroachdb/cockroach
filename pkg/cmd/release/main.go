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
	"context"
	"encoding/base64"
	"encoding/json"
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
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	version "github.com/hashicorp/go-version"
	isatty "github.com/mattn/go-isatty"
)

const (
	dockerEmailKey         = "DOCKER_EMAIL"
	dockerAuthKey          = "DOCKER_AUTH"
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
	branch, ok := os.LookupEnv(teamcityBuildBranchKey)
	if !ok {
		log.Fatalf("VCS branch environment variable %s is not set", teamcityBuildBranchKey)
	}
	isHead := branch == "master"
	pkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		log.Fatal(err)
	}
	var versionStr string
	switch {
	case isHead:
		cmd := exec.Command("git", "rev-parse", "HEAD")
		cmd.Dir = pkg.Dir
		out, err := cmd.Output()
		if err != nil {
			log.Fatal(err)
		}
		versionStr = string(out)
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
			{suffix: "deadlock", tags: "deadlock"},
			{suffix: "race", goflags: "-race"},
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
				args = append(args, fmt.Sprintf("%s=%s", "GOFLAGS", extraArgs.goflags))
				args = append(args, fmt.Sprintf("%s=%s", "SUFFIX", extraArgs.suffix))
				args = append(args, fmt.Sprintf("%s=%s", "TAGS", extraArgs.tags))
				cmd := exec.Command("make", args...)
				cmd.Dir = pkg.Dir
				if err := cmd.Run(); err != nil {
					log.Fatal(err)
				}
			}

			if strings.Contains(buildType, "linux") {
				binaryName := fmt.Sprintf("cockroach%s", extraArgs.suffix)

				cmd := exec.Command(binaryName, "version")
				cmd.Dir = pkg.Dir
				cmd.Env = append(cmd.Env, "MALLOC_CONF=prof:true")
				if err := cmd.Run(); err != nil {
					log.Fatal(err)
				}

				// ldd only works on binaries built for the host. "linux-musl"
				// produces fully static binaries, which cause ldd to exit
				// non-zero.
				//
				// TODO(tamird): implement this for all targets.
				if !strings.HasSuffix(buildType, "linux-musl") {
					cmd := exec.Command("ldd", binaryName)
					cmd.Dir = pkg.Dir
					out, err := cmd.Output()
					if err != nil {
						log.Fatal(err)
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

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	uploader := s3manager.NewUploader(sess)

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

			f, err := os.Open(filepath.Join(pkg.Dir, relativePath))
			if err != nil {
				log.Fatal(err)
			}
			versionKey := fmt.Sprintf("%s/%s.%s", repoName, remoteName, versionStr)
			output, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: &bucketName,
				Key:    &versionKey,
				Body:   f,
			})
			if err != nil {
				log.Fatal(err)
			}
			if err := f.Close(); err != nil {
				log.Fatal(err)
			}
			latestKey := fmt.Sprintf("%s/%s.%s", repoName, remoteName, "LATEST")
			if _, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: &bucketName,
				Key:    &latestKey,
				WebsiteRedirectLocation: &output.Location,
			}); err != nil {
				log.Fatal(err)
			}
		}
	} else {
		// Can't be const because we take its address.
		bucketName := "binaries.cockroachdb.com"

		versionStrs := []string{versionStr, "latest"}

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
			if err := cmd.Run(); err != nil {
				log.Fatal(err)
			}

			f, err := os.Open(srcArchive)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: &bucketName,
				Key:    &srcArchive,
				Body:   f,
			}); err != nil {
				log.Fatal(err)
			}
			if err := f.Close(); err != nil {
				log.Fatal(err)
			}

			for relativePath, targetSuffix := range map[string]string{
				"cockroach-darwin-10.9-amd64": "darwin-10.9-amd64",
				// TODO(tamird): make this linux-gnu-amd64 and add versions to
				// both Linux targets.
				"cockroach-linux-2.6.32-gnu-amd64":  "linux-amd64",
				"cockroach-linux-2.6.32-musl-amd64": "linux-musl-amd64",
				"cockroach-windows-6.2-amd64.exe":   "windows-6.2-amd64",
			} {
				targetArchive := fmt.Sprintf("%s.%s", archiveBase, targetSuffix)
				binary, err := os.Open(filepath.Join(pkg.Dir, relativePath))
				if err != nil {
					log.Fatal(err)
				}
				var f *os.File
				if strings.HasSuffix(relativePath, ".exe") {
					f, err = os.Create(targetArchive + ".zip")
					if err != nil {
						log.Fatal(err)
					}
					zw := zip.NewWriter(f)
					zfw, err := zw.Create(filepath.Join(targetArchive, "cockroach.exe"))
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
					f, err = os.Create(targetArchive + ".tgz")
					if err != nil {
						log.Fatal(err)
					}
					gzw := gzip.NewWriter(f)
					tw := tar.NewWriter(gzw)
					binaryInfo, err := binary.Stat()
					if err != nil {
						log.Fatal(err)
					}
					if err := tw.WriteHeader(&tar.Header{
						Name: filepath.Join(targetArchive, "cockroach"),
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

				if _, err := uploader.Upload(&s3manager.UploadInput{
					Bucket: &bucketName,
					Key:    &targetArchive,
					Body:   f,
				}); err != nil {
					log.Fatal(err)
				}
				if err := f.Close(); err != nil {
					log.Fatal(err)
				}
			}
		}

		dockerAuth, ok := os.LookupEnv(dockerAuthKey)
		if !ok {
			log.Fatalf("docker auth environment variable %s is not set", dockerAuthKey)
		}
		dockerEmail, ok := os.LookupEnv(dockerEmailKey)
		if !ok {
			log.Fatalf("docker auth environment variable %s is not set", dockerEmailKey)
		}
		authConfig := types.AuthConfig{
			Auth:  dockerAuth,
			Email: dockerEmail,
		}
		// This is the same as
		// github.com/docker/docker/cli/command.EncodeAuthToBase64, but that
		// package depends on a version of github.com/docker/distribution
		// preceding https://github.com/docker/distribution/commit/7dba427, so
		// we save some additional dependency pinning by avoiding that
		// package.
		buf, err := json.Marshal(authConfig)
		if err != nil {
			log.Fatal(err)
		}
		encodedAuth := base64.URLEncoding.EncodeToString(buf)
		cmd := exec.Command("cp", "cockroach-linux-2.6.32-gnu-amd64", filepath.Join("build", "deploy", "cockroach"))
		cmd.Dir = pkg.Dir
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
		cli, err := client.NewEnvClient()
		if err != nil {
			log.Fatal(err)
		}
		ctx := context.Background()
		const image = "cockroachdb/cockroach"
		var tags []string
		for _, tag := range versionStrs {
			tags = append(tags, fmt.Sprintf("%s:%s", image, tag))
		}
		resp, err := cli.ImageBuild(ctx, nil, types.ImageBuildOptions{
			Tags:       tags,
			Dockerfile: filepath.Join(pkg.Dir, "build", "deploy", "Dockerfile"),
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := displayDockerStream(resp.Body); err != nil {
			log.Fatal(err)
		}
		rc, err := cli.ImagePush(ctx, image, types.ImagePushOptions{
			RegistryAuth: encodedAuth,
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := displayDockerStream(rc); err != nil {
			log.Fatal(err)
		}
	}
}

func displayDockerStream(rc io.ReadCloser) error {
	defer rc.Close()
	out := os.Stderr
	outFd := out.Fd()
	isTerminal := isatty.IsTerminal(outFd)
	return jsonmessage.DisplayJSONMessagesStream(rc, out, outFd, isTerminal, nil)
}
