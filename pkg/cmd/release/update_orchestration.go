// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// updateOrchestration updates the kubernetes manifests in the cockroach repo using templates.
func updateOrchestration(gitDir string, version string) error {
	// make sure we have the leading "v" in the version
	version = "v" + strings.TrimPrefix(version, "v")
	// Switch to the git directory to prevent file prefixes in the generated templates.
	currDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting current directory: %w", err)
	}
	if err := os.Chdir(gitDir); err != nil {
		return fmt.Errorf("switching to git directory: %w", err)
	}
	defer func() {
		_ = os.Chdir(currDir)
	}()

	const templatesDir = "cloud/kubernetes/templates"
	const outputDir = "cloud/kubernetes"
	dirInfo, err := os.Stat(templatesDir)
	if err != nil {
		return fmt.Errorf("cannot stat templates directory: %w", err)
	}
	if !dirInfo.IsDir() {
		return fmt.Errorf("%s is not a directory", templatesDir)
	}
	return filepath.Walk(templatesDir, func(filePath string, fileInfo os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		// Skip directories
		if !fileInfo.Mode().IsRegular() {
			return nil
		}
		// calculate file directory relative to the given root directory.
		dir := path.Dir(filePath)
		relDir, err := filepath.Rel(templatesDir, dir)
		if err != nil {
			return err
		}
		destDir := filepath.Join(outputDir, relDir)
		destFile := filepath.Join(destDir, fileInfo.Name())
		if err := os.MkdirAll(destDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		contents, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		// Go templates cannot be used here, because some files are templates already.
		generatedContents := strings.ReplaceAll(string(contents), "@VERSION@", version)
		if strings.HasSuffix(destFile, ".yaml") {
			generatedContents = fmt.Sprintf("# Generated file, DO NOT EDIT. Source: %s\n", filePath) + generatedContents
		}
		err = os.WriteFile(destFile, []byte(generatedContents), fileInfo.Mode().Perm())
		if err != nil {
			return err
		}
		return nil
	})
}
