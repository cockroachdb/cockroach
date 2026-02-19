// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors/oserror"
)

// Parallel test execution:
// Tests in this package run concurrently. The CockroachDB linter flags
// the parallel testing API due to golang/go#31651 (deferred cleanup
// timing with parallel subtests). That issue does not apply here:
// parallelism is on top-level test functions (not subtests), and cleanup
// uses t.Cleanup which waits for parallel tests to complete. Each call
// is annotated with "SAFE FOR TESTING" to satisfy the linter.

// TestMain handles one-time authentication setup before any tests run.
//
// Roachprod uses both the Google Cloud Go SDK (for GCE API calls) and the
// gcloud CLI (for operations where roachprod shells out), so both need to
// be authenticated.
//
// Two paths:
//   - CI: GOOGLE_EPHEMERAL_CREDENTIALS contains service account JSON.
//     We write it to a file, set GOOGLE_APPLICATION_CREDENTIALS for the SDK,
//     and run `gcloud auth activate-service-account` for the CLI.
//   - Local: No ephemeral credentials. We copy the developer's existing
//     gcloud config and SSH keys from their real home directory into the
//     Bazel test sandbox (HOME is set to TEST_TMPDIR by BUILD.bazel).
//
// After auth, we ensure an SSH keypair exists (roachprod needs one to
// connect to VMs).
func TestMain(m *testing.M) {
	if creds := os.Getenv("GOOGLE_EPHEMERAL_CREDENTIALS"); creds != "" {
		setupCIAuth(creds, os.Getenv("GOOGLE_PROJECT"))
	} else {
		setupLocalAuth()
	}
	ensureSSHKey()
	os.Exit(m.Run())
}

// setupCIAuth configures Google Cloud authentication from ephemeral credentials
// (service account JSON). Sets up both the Go SDK (via GOOGLE_APPLICATION_CREDENTIALS)
// and the gcloud CLI (via activate-service-account).
func setupCIAuth(credsJSON, project string) {
	fmt.Fprintf(os.Stderr, "TestMain: setting up CI authentication\n")

	// Set CLOUDSDK_CONFIG to give gcloud CLI a writable config directory.
	// Without this, gcloud would try to write to ~/.config/gcloud which
	// may not exist or be writable in the Bazel test sandbox.
	if os.Getenv("CLOUDSDK_CONFIG") == "" {
		testTmpDir := os.Getenv("TEST_TMPDIR")
		if testTmpDir == "" {
			testTmpDir = os.TempDir()
		}
		cloudSDKConfig := filepath.Join(testTmpDir, "gcloud_config")
		if err := os.MkdirAll(cloudSDKConfig, 0755); err != nil {
			panic("failed to create gcloud config directory: " + err.Error())
		}
		if err := os.Setenv("CLOUDSDK_CONFIG", cloudSDKConfig); err != nil {
			panic("failed to set CLOUDSDK_CONFIG: " + err.Error())
		}
	}

	// Write credentials to a temp file for both SDK and CLI.
	tmpDir := os.Getenv("TEST_TMPDIR")
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	credsFile := filepath.Join(tmpDir, "gcloud-credentials.json")
	if err := os.WriteFile(credsFile, []byte(credsJSON), 0600); err != nil {
		panic("failed to write GCloud credentials: " + err.Error())
	}

	// Go SDK: set GOOGLE_APPLICATION_CREDENTIALS so Google Cloud Go client
	// libraries can find the service account credentials.
	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile); err != nil {
		panic("failed to set GOOGLE_APPLICATION_CREDENTIALS: " + err.Error())
	}

	// gcloud CLI: authenticate and add to PATH.
	// The full path is used because the Bazel test sandbox may not have
	// /google-cloud-sdk/bin in PATH. This path matches the CI Docker
	// container (see build/bazelbuilder/Dockerfile).
	const gcloudDir = "/google-cloud-sdk/bin"
	const gcloudPath = gcloudDir + "/gcloud"

	if err := os.Setenv("PATH", gcloudDir+":"+os.Getenv("PATH")); err != nil {
		panic("failed to update PATH: " + err.Error())
	}

	cmd := exec.Command(gcloudPath, "auth", "activate-service-account", "--key-file="+credsFile)
	if output, err := cmd.CombinedOutput(); err != nil {
		panic("failed to activate service account: " + err.Error() + "\nOutput: " + string(output))
	}

	if project != "" {
		cmd := exec.Command(gcloudPath, "config", "set", "project", project)
		if output, err := cmd.CombinedOutput(); err != nil {
			panic("failed to set Google Cloud project: " + err.Error() + "\nOutput: " + string(output))
		}
	}
}

// setupLocalAuth copies the developer's gcloud config and SSH keys from their
// real home directory into the Bazel test sandbox. This is needed because
// BUILD.bazel sets HOME=TEST_TMPDIR, so roachprod can't find ~/.config/gcloud
// or ~/.ssh at their usual locations.
func setupLocalAuth() {
	fmt.Fprintf(os.Stderr, "TestMain: setting up local authentication\n")

	// Detect the real home directory. os.UserHomeDir() would return
	// TEST_TMPDIR (since Bazel overrides HOME), so we check REAL_HOME
	// first, then fall back to /Users/<user> or /home/<user>.
	realHome := os.Getenv("REAL_HOME")
	if realHome == "" {
		if user := os.Getenv("USER"); user != "" {
			realHome = "/Users/" + user
			if _, err := os.Stat(realHome); oserror.IsNotExist(err) {
				realHome = "/home/" + user
			}
		}
	}
	if realHome == "" {
		return
	}

	testHome := os.Getenv("HOME")
	if testHome == "" {
		testHome = os.Getenv("TEST_TMPDIR")
	}
	if testHome == "" || testHome == realHome {
		return
	}

	// Copy gcloud config so roachprod can use existing gcloud credentials.
	srcGcloudConfig := filepath.Join(realHome, ".config", "gcloud")
	if _, err := os.Stat(srcGcloudConfig); err == nil {
		dstGcloudConfig := filepath.Join(testHome, ".config", "gcloud")
		if err := copyDir(srcGcloudConfig, dstGcloudConfig); err != nil {
			panic("failed to copy gcloud config: " + err.Error())
		}
	}

	// Copy SSH keys so roachprod can connect to VMs.
	srcSSHDir := filepath.Join(realHome, ".ssh")
	if _, err := os.Stat(srcSSHDir); err == nil {
		dstSSHDir := filepath.Join(testHome, ".ssh")
		if err := copyDir(srcSSHDir, dstSSHDir); err != nil {
			panic("failed to copy SSH keys: " + err.Error())
		}
	}
}

// ensureSSHKey generates an SSH keypair if one doesn't exist in the test HOME.
// Roachprod requires an SSH key to connect to cluster VMs.
func ensureSSHKey() {
	testHome := os.Getenv("HOME")
	if testHome == "" {
		testHome = os.Getenv("TEST_TMPDIR")
	}
	if testHome == "" {
		return
	}

	sshKeyPath := filepath.Join(testHome, ".ssh", "id_rsa")
	if _, err := os.Stat(sshKeyPath); !oserror.IsNotExist(err) {
		return
	}

	sshDir := filepath.Join(testHome, ".ssh")
	if err := os.MkdirAll(sshDir, 0700); err != nil {
		panic("failed to create .ssh directory: " + err.Error())
	}
	cmd := exec.Command("ssh-keygen", "-t", "rsa", "-N", "", "-f", sshKeyPath, "-C", "roachprod-test")
	if output, err := cmd.CombinedOutput(); err != nil {
		panic("failed to generate SSH key: " + err.Error() + "\nOutput: " + string(output))
	}
}

// copyDir recursively copies a directory tree.
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		dstFile, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}
		defer dstFile.Close()

		_, err = io.Copy(dstFile, srcFile)
		return err
	})
}
