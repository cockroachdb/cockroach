// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// TemplateMetadata holds the name and description parsed from a template's
// template.yaml or template.yml marker file.
type TemplateMetadata struct {
	Name            string `yaml:"name" json:"name"`
	Description     string `yaml:"description,omitempty" json:"description,omitempty"`
	DefaultLifetime string `yaml:"default_lifetime,omitempty" json:"default_lifetime,omitempty"`
}

// Template represents a discovered terraform template with its parsed variable
// schemas.
type Template struct {
	TemplateMetadata
	// DirName is the directory name under the templates root. This is the
	// canonical lookup key used by GetTemplate, inspect, and snapshot.
	DirName   string                   `json:"dir_name"`
	Variables map[string]TemplateOption `json:"variables"`
	Checksum  string                   `json:"checksum,omitempty"`
	Path      string                   `json:"-"`
}

// TemplateOption is a recursive type that fully describes an HCL variable's
// type and defaults. It is the foundation for variable assembly and
// serialization to -var flags.
//
// The Value field is interface{} but the concrete types stored are well-defined:
//   - string for Type == "string"
//   - float64 for Type == "number"
//   - bool for Type == "bool"
//   - []TemplateOption for Type == "list", "set", "tuple"
//   - map[string]TemplateOption for Type == "object", "map"
//   - nil when there is no default (variable is required)
type TemplateOption struct {
	// Type is the simplified type name used by variable assembly logic
	// ("string", "number", "bool", "list", "set", "tuple", "object", "map").
	Type string `json:"type"`
	// FullType is the complete HCL type constraint string, preserving optional()
	// wrappers and nested structure (e.g.
	// "object({region=optional(string,"us-east1"),…})").
	// Only set on top-level variables, not on recursive children.
	FullType    string                    `json:"full_type,omitempty"`
	Value       interface{}               `json:"default,omitempty"`
	InnerTypes  []TemplateOption          `json:"inner_types,omitempty"`
	Required    bool                      `json:"required"`
	Sensitive   bool                      `json:"sensitive,omitempty"`
	Description string                    `json:"description,omitempty"`
}

// GetAsInterface recursively converts a TemplateOption tree back to native
// Go types (string, float64, bool, []interface{}, map[string]interface{}).
func (opt TemplateOption) GetAsInterface() interface{} {
	switch opt.Type {
	case "string":
		return opt.Value
	case "number":
		switch v := opt.Value.(type) {
		case float64:
			return v
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil
			}
			return f
		}
		return nil
	case "bool":
		switch v := opt.Value.(type) {
		case bool:
			return v
		case string:
			return v == "true"
		}
		return nil
	case "list", "set", "tuple":
		if elems, ok := opt.Value.([]TemplateOption); ok {
			result := make([]interface{}, 0, len(elems))
			for _, elem := range elems {
				result = append(result, elem.GetAsInterface())
			}
			return result
		}
		return []interface{}{}
	case "object", "map":
		if attrs, ok := opt.Value.(map[string]TemplateOption); ok {
			result := make(map[string]interface{}, len(attrs))
			for k, v := range attrs {
				result[k] = v.GetAsInterface()
			}
			return result
		}
		return map[string]interface{}{}
	}
	return nil
}

// ToStringMap converts a map of TemplateOptions to flat key=value pairs
// suitable for -var flags. Complex types are JSON-serialized.
func ToStringMap(opts map[string]TemplateOption) (map[string]string, error) {
	result := make(map[string]string)
	for name, opt := range opts {
		val := opt.GetAsInterface()
		switch v := val.(type) {
		case string:
			result[name] = v
		case float64:
			result[name] = fmt.Sprintf("%g", v)
		case bool:
			result[name] = fmt.Sprintf("%t", v)
		case nil:
			continue
		default:
			data, err := json.Marshal(val)
			if err != nil {
				return nil, fmt.Errorf("marshal variable %s: %w", name, err)
			}
			result[name] = string(data)
		}
	}
	return result, nil
}
