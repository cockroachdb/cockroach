// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/zclconf/go-cty/cty"
)

// moduleSourceSchema extracts only the source attribute from a module block.
// PartialContent is used so that other attributes (version, providers, etc.)
// are silently ignored.
var moduleSourceSchema = &hcl.BodySchema{
	Attributes: []hcl.AttributeSchema{
		{Name: "source"},
	},
}

// ValidateModuleReferences checks that all relative module source paths in
// the template directory (and recursively in referenced modules) resolve to
// existing directories. Returns a list of human-readable warnings for any
// broken references. An empty slice means all references are valid.
//
// This catches a common template authoring mistake: symlink-ing a shared
// module into the template tree but forgetting to also symlink the sibling
// modules that the shared module references via relative paths.
func ValidateModuleReferences(templateDir string) []string {
	visited := make(map[string]bool)
	var warnings []string
	validateModuleDir(templateDir, templateDir, visited, &warnings)
	return warnings
}

// validateModuleDir scans all .tf files in dir for module blocks with
// relative source paths and checks that each target directory exists.
// Found modules are validated recursively; visited tracks absolute paths
// to avoid infinite loops.
func validateModuleDir(
	dir, templateRoot string, visited map[string]bool, warnings *[]string,
) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return
	}
	if visited[absDir] {
		return
	}
	visited[absDir] = true

	parser := hclparse.NewParser()

	files, err := listTFFiles(dir)
	if err != nil {
		return
	}

	for _, file := range files {
		hclFile, diags := parser.ParseHCLFile(file)
		if diags.HasErrors() {
			continue
		}

		content, diags := hclFile.Body.Content(configFileSchema)
		if diags.HasErrors() {
			continue
		}

		for _, block := range content.Blocks {
			if block.Type != "module" {
				continue
			}

			moduleContent, _, _ := block.Body.PartialContent(moduleSourceSchema)
			srcAttr, exists := moduleContent.Attributes["source"]
			if !exists {
				continue
			}

			val, valDiags := srcAttr.Expr.Value(nil)
			if valDiags.HasErrors() || val.Type() != cty.String {
				continue
			}
			source := val.AsString()

			// Only validate relative paths. Registry, git, and HTTP sources
			// are resolved by tofu init and can't be checked locally.
			if !strings.HasPrefix(source, "./") && !strings.HasPrefix(source, "../") {
				continue
			}

			resolved := filepath.Join(filepath.Dir(file), source)
			info, statErr := os.Stat(resolved)
			if statErr != nil || !info.IsDir() {
				relFile, _ := filepath.Rel(templateRoot, file)
				*warnings = append(*warnings, fmt.Sprintf(
					"%s: module %q references %q which does not exist "+
						"(resolved to %s)",
					relFile, block.Labels[0], source, resolved,
				))
				continue
			}

			// Recursively validate the referenced module directory.
			validateModuleDir(resolved, templateRoot, visited, warnings)
		}
	}
}
