// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package test

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	// Set up Google Cloud authentication for TeamCity/CI environments
	// The test framework needs gcloud CLI to be authenticated for roachprod to work
	credsJSON := os.Getenv("GOOGLE_EPHEMERAL_CREDENTIALS")
	googleProject := os.Getenv("GOOGLE_PROJECT")

	// Handle CI authentication (JSON credentials in env var)
	// Roachprod uses both Google Cloud Go SDK libraries AND gcloud CLI commands,
	// so we need to set up authentication for both.
	if credsJSON != "" {
		// === gcloud CLI setup ===
		// Set CLOUDSDK_CONFIG to give gcloud CLI a writable directory for its config.
		// Without this, gcloud commands would try to write to ~/.config/gcloud which
		// may not exist or be writable in the bazel test sandbox.
		if os.Getenv("CLOUDSDK_CONFIG") == "" {
			testTmpDir := os.Getenv("TEST_TMPDIR")
			if testTmpDir == "" {
				testTmpDir = os.TempDir()
			}
			cloudSDKConfig := testTmpDir + "/gcloud_config"
			if err := os.MkdirAll(cloudSDKConfig, 0755); err != nil {
				panic("Failed to create gcloud config directory: " + err.Error())
			}
			if err := os.Setenv("CLOUDSDK_CONFIG", cloudSDKConfig); err != nil {
				panic("Failed to set CLOUDSDK_CONFIG: " + err.Error())
			}
		}

		// Write credentials to a file (needed by both SDK and CLI)
		tmpFile := "/tmp/gcloud-credentials.json"
		if err := os.WriteFile(tmpFile, []byte(credsJSON), 0600); err != nil {
			panic("Failed to write GCloud credentials: " + err.Error())
		}

		// === Go SDK setup ===
		// Set GOOGLE_APPLICATION_CREDENTIALS so Google Cloud Go SDK libraries
		// (used by roachprod) can find and use the service account credentials.
		if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", tmpFile); err != nil {
			panic("Failed to set GOOGLE_APPLICATION_CREDENTIALS: " + err.Error())
		}

		// === gcloud CLI authentication ===
		// Authenticate the gcloud CLI tool in case roachprod shells out to gcloud commands.
		// Use full path to gcloud since bazel test sandbox may not have it in PATH.
		gcloudPath := "/google-cloud-sdk/bin/gcloud"

		// Add gcloud to PATH so roachprod can find it when it shells out to gcloud
		currentPath := os.Getenv("PATH")
		gcloudDir := "/google-cloud-sdk/bin"
		if err := os.Setenv("PATH", gcloudDir+":"+currentPath); err != nil {
			panic("Failed to update PATH: " + err.Error())
		}

		cmd := exec.Command(gcloudPath, "auth", "activate-service-account", "--key-file="+tmpFile)
		if output, err := cmd.CombinedOutput(); err != nil {
			panic("Failed to activate service account: " + err.Error() + "\nOutput: " + string(output))
		}

		// Set Google Cloud project for gcloud CLI
		if googleProject != "" {
			cmd := exec.Command(gcloudPath, "config", "set", "project", googleProject)
			if output, err := cmd.CombinedOutput(); err != nil {
				panic("Failed to set Google Cloud project: " + err.Error() + "\nOutput: " + string(output))
			}
		}
	} else {
		// Local development: Copy user's gcloud config to test HOME
		// This is needed because bazel sets HOME to TEST_TMPDIR
		realHome := os.Getenv("REAL_HOME")
		if realHome == "" {
			// Try to detect real home from USER env var
			if user := os.Getenv("USER"); user != "" {
				realHome = "/Users/" + user // macOS/Linux assumption
				if _, err := os.Stat(realHome); os.IsNotExist(err) {
					realHome = "/home/" + user // Linux
				}
			}
		}

		if realHome != "" {
			srcGcloudConfig := filepath.Join(realHome, ".config", "gcloud")
			testHome := os.Getenv("HOME")
			if testHome == "" {
				testHome = os.Getenv("TEST_TMPDIR")
			}
			if testHome != "" && testHome != realHome {
				dstGcloudConfig := filepath.Join(testHome, ".config", "gcloud")

				// Copy gcloud config if source exists
				if _, err := os.Stat(srcGcloudConfig); err == nil {
					if err := copyDir(srcGcloudConfig, dstGcloudConfig); err != nil {
						panic("Failed to copy gcloud config: " + err.Error())
					}
				}

				// Copy SSH keys if they exist
				srcSSHDir := filepath.Join(realHome, ".ssh")
				dstSSHDir := filepath.Join(testHome, ".ssh")
				if _, err := os.Stat(srcSSHDir); err == nil {
					if err := copyDir(srcSSHDir, dstSSHDir); err != nil {
						panic("Failed to copy SSH keys: " + err.Error())
					}
				}
			}
		}
	}

	// Generate SSH key if it doesn't exist (needed for roachprod)
	testHome := os.Getenv("HOME")
	if testHome == "" {
		testHome = os.Getenv("TEST_TMPDIR")
	}
	if testHome != "" {
		sshDir := filepath.Join(testHome, ".ssh")
		sshKeyPath := filepath.Join(sshDir, "id_rsa")
		if _, err := os.Stat(sshKeyPath); os.IsNotExist(err) {
			// Generate SSH key using ssh-keygen
			if err := os.MkdirAll(sshDir, 0700); err != nil {
				panic("Failed to create .ssh directory: " + err.Error())
			}
			cmd := exec.Command("ssh-keygen", "-t", "rsa", "-N", "", "-f", sshKeyPath, "-C", "roachprod-test")
			if output, err := cmd.CombinedOutput(); err != nil {
				panic("Failed to generate SSH key: " + err.Error() + "\nOutput: " + string(output))
			}
		}
	}

	// Run tests
	os.Exit(m.Run())
}

// copyDir recursively copies a directory tree
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Copy file
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
