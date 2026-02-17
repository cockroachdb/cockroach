// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

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
