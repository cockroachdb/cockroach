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
// It first tries an exact directory name match, then falls back to scanning
// all templates for a matching metadata name.
func (m *Manager) GetTemplate(name string) (provisionings.Template, error) {
	// Try direct directory name lookup first.
	tmpl, err := m.getTemplateByDir(name)
	if err == nil {
		return tmpl, nil
	}

	// Fall back to metadata name lookup.
	tmpl, metaErr := m.getTemplateByMetaName(name)
	if metaErr == nil {
		return tmpl, nil
	}

	return provisionings.Template{}, errors.Newf("template %q not found", name)
}

// getTemplateByDir loads a template from a specific subdirectory.
func (m *Manager) getTemplateByDir(dirName string) (provisionings.Template, error) {
	templateDir := filepath.Join(m.templatesDir, dirName)
	info, err := os.Stat(templateDir)
	if err != nil {
		return provisionings.Template{}, err
	}
	if !info.IsDir() {
		return provisionings.Template{}, errors.Newf("template %q is not a directory", dirName)
	}

	meta, found := readTemplateMarker(templateDir)
	if !found {
		return provisionings.Template{}, errors.Newf(
			"template %q has no template.yaml or template.yml marker", dirName,
		)
	}

	vars, parseErr := ParseTemplateVariables(templateDir)
	if parseErr != nil {
		return provisionings.Template{}, errors.Wrapf(
			parseErr, "parse variables for template %s", dirName,
		)
	}

	return provisionings.Template{
		TemplateMetadata: meta,
		DirName:          dirName,
		Variables:        vars,
		Path:             templateDir,
	}, nil
}

// getTemplateByMetaName scans all templates and returns the first one whose
// metadata name matches.
func (m *Manager) getTemplateByMetaName(name string) (provisionings.Template, error) {
	templates, err := m.ListTemplates()
	if err != nil {
		return provisionings.Template{}, err
	}
	for _, tmpl := range templates {
		if tmpl.Name == name {
			return tmpl, nil
		}
	}
	return provisionings.Template{}, errors.Newf("template with metadata name %q not found", name)
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
