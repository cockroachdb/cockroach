// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

// ErrNoTemplateMarker is returned by ParseMetadataFromDir when neither
// template.yaml nor template.yml exists in the directory. Callers can use
// errors.Is to distinguish "no marker" (safe to skip hooks) from parse
// errors (should fail).
var ErrNoTemplateMarker = errors.New("no template marker file found")

// ITemplateManager defines the interface for template discovery and management.
type ITemplateManager interface {
	ListTemplates() ([]provisionings.Template, error)
	GetTemplate(name string) (provisionings.Template, error)
	SnapshotTemplate(name string) (archive []byte, checksum string, err error)
	ExtractSnapshot(archive []byte, workingDir string) error
}

// Manager discovers and parses terraform templates from a directory.
type Manager struct {
	templatesDir string
}

// NewManager creates a new template manager rooted at templatesDir.
func NewManager(templatesDir string) *Manager {
	return &Manager{templatesDir: templatesDir}
}

// GetTemplatesDir returns the configured templates directory.
func (m *Manager) GetTemplatesDir() string {
	return m.templatesDir
}

// ListTemplates scans the templates directory for subdirectories containing a
// template.yaml or template.yml marker file.
func (m *Manager) ListTemplates() ([]provisionings.Template, error) {
	entries, err := os.ReadDir(m.templatesDir)
	if err != nil {
		return nil, errors.Wrapf(err, "read templates directory %s", m.templatesDir)
	}

	var templates []provisionings.Template
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, found := readTemplateMarker(filepath.Join(m.templatesDir, entry.Name()))
		if !found {
			continue
		}

		templateDir := filepath.Join(m.templatesDir, entry.Name())
		vars, parseErr := ParseTemplateVariables(templateDir)
		if parseErr != nil {
			return nil, errors.Wrapf(parseErr, "parse variables for template %s", entry.Name())
		}

		templates = append(templates, provisionings.Template{
			TemplateMetadata: meta,
			DirName:          entry.Name(),
			Variables:        vars,
			Path:             templateDir,
		})
	}

	return templates, nil
}

// GetTemplate returns a single template by directory name or metadata name.
// Resolution is performed only against discovered templates from ListTemplates,
// so user input is never used to build direct filesystem paths.
func (m *Manager) GetTemplate(name string) (provisionings.Template, error) {
	templates, err := m.ListTemplates()
	if err != nil {
		return provisionings.Template{}, err
	}

	// Support both aliases:
	// - directory name (`DirName`)
	// - template metadata name (`Name`)
	for _, tmpl := range templates {
		if tmpl.DirName == name || tmpl.Name == name {
			return tmpl, nil
		}
	}

	return provisionings.Template{}, errors.Newf("template %q not found", name)
}

// readTemplateMarker looks for template.yaml or template.yml in dir and parses
// the metadata. Returns the metadata and true if found, or zero value and false
// if no marker file exists.
func readTemplateMarker(dir string) (provisionings.TemplateMetadata, bool) {
	for _, name := range []string{"template.yaml", "template.yml"} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var meta provisionings.TemplateMetadata
		if yamlErr := yaml.Unmarshal(data, &meta); yamlErr != nil {
			continue
		}
		if meta.Name == "" {
			continue
		}
		return meta, true
	}
	return provisionings.TemplateMetadata{}, false
}

// ParseMetadataFromDir reads template.yaml or template.yml from a filesystem
// directory and returns the parsed metadata. Used by HandleProvision where the
// working directory already contains the extracted snapshot.
//
// Unlike readTemplateMarker (used during template discovery, where skipping
// malformed files is acceptable), this function distinguishes "file not found"
// from "file found but malformed" and returns an error for the latter.
func ParseMetadataFromDir(dir string) (provisionings.TemplateMetadata, error) {
	for _, name := range []string{"template.yaml", "template.yml"} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // file does not exist, try next
			}
			return provisionings.TemplateMetadata{}, errors.Wrapf(
				err, "read %s in %s", name, dir,
			)
		}
		// File exists — parse errors are real failures, not "no hooks."
		var meta provisionings.TemplateMetadata
		if yamlErr := yaml.Unmarshal(data, &meta); yamlErr != nil {
			return provisionings.TemplateMetadata{}, errors.Wrapf(
				yamlErr, "parse %s in %s", name, dir,
			)
		}
		if meta.Name == "" {
			return provisionings.TemplateMetadata{}, errors.Newf(
				"%s in %s has empty name field", name, dir,
			)
		}
		return meta, nil
	}
	return provisionings.TemplateMetadata{}, errors.Wrapf(
		ErrNoTemplateMarker,
		"no template.yaml or template.yml in %s", dir,
	)
}

// ParseMetadataFromSnapshot extracts and parses template.yaml from a tar.gz
// archive in memory without writing to disk. Used by HandleSetupSSHKeys
// (Commit 12) where no working directory exists.
func ParseMetadataFromSnapshot(archive []byte) (provisionings.TemplateMetadata, error) {
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return provisionings.TemplateMetadata{}, errors.Wrap(
			err, "open gzip reader for metadata extraction",
		)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return provisionings.TemplateMetadata{}, errors.Wrap(
				err, "read tar entry for metadata extraction",
			)
		}

		if header.Name != "template.yaml" && header.Name != "template.yml" {
			continue
		}

		data, err := io.ReadAll(tr)
		if err != nil {
			return provisionings.TemplateMetadata{}, errors.Wrapf(
				err, "read %s from snapshot", header.Name,
			)
		}

		var meta provisionings.TemplateMetadata
		if err := yaml.Unmarshal(data, &meta); err != nil {
			return provisionings.TemplateMetadata{}, errors.Wrapf(
				err, "unmarshal %s from snapshot", header.Name,
			)
		}
		if meta.Name == "" {
			return provisionings.TemplateMetadata{}, errors.Newf(
				"%s in snapshot has empty name", header.Name,
			)
		}
		return meta, nil
	}

	return provisionings.TemplateMetadata{}, errors.New(
		"no template.yaml or template.yml found in snapshot",
	)
}
