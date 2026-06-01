// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/ext/typeexpr"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

// configFileSchema defines the top-level blocks we expect in a .tf file.
// We list all standard block types so HCL's partial content extraction
// does not error on non-variable blocks.
var configFileSchema = &hcl.BodySchema{
	Blocks: []hcl.BlockHeaderSchema{
		{Type: "terraform"},
		{Type: "required_providers"},
		{Type: "provider", LabelNames: []string{"name"}},
		{Type: "variable", LabelNames: []string{"name"}},
		{Type: "locals"},
		{Type: "output", LabelNames: []string{"name"}},
		{Type: "module", LabelNames: []string{"name"}},
		{Type: "resource", LabelNames: []string{"type", "name"}},
		{Type: "data", LabelNames: []string{"type", "name"}},
		{Type: "moved"},
	},
}

// variableBlockSchema defines the attributes we extract from a variable block.
var variableBlockSchema = &hcl.BodySchema{
	Attributes: []hcl.AttributeSchema{
		{Name: "description"},
		{Name: "default"},
		{Name: "type"},
		{Name: "sensitive"},
		{Name: "nullable"},
	},
	Blocks: []hcl.BlockHeaderSchema{
		{Type: "validation"},
	},
}

// ParseTemplateVariables parses all .tf files in dir and extracts variable
// blocks into a map of TemplateOption keyed by variable name.
func ParseTemplateVariables(dir string) (map[string]provisionings.TemplateOption, error) {
	parser := hclparse.NewParser()

	files, err := listTFFiles(dir)
	if err != nil {
		return nil, errors.Wrap(err, "list terraform files")
	}

	vars := make(map[string]provisionings.TemplateOption)
	for _, file := range files {
		hclFile, diags := parser.ParseHCLFile(file)
		if diags.HasErrors() {
			slog.Warn("skipping file with parse errors",
				slog.String("file", file),
				slog.String("error", diags.Error()),
			)
			continue
		}

		content, contentDiags := hclFile.Body.Content(configFileSchema)
		if contentDiags.HasErrors() {
			slog.Warn("skipping file with content errors",
				slog.String("file", file),
				slog.String("error", contentDiags.Error()),
			)
			continue
		}

		for _, block := range content.Blocks {
			if block.Type != "variable" {
				continue
			}
			name, opt, parseErr := parseVariable(block)
			if parseErr != nil {
				return nil, errors.Wrapf(parseErr, "parse variable %q in %s", block.Labels[0], file)
			}
			vars[name] = opt
		}
	}

	return vars, nil
}

// parseVariable extracts the type, default value, description, and sensitive
// flag from a single HCL variable block.
func parseVariable(block *hcl.Block) (string, provisionings.TemplateOption, error) {
	content, _ := block.Body.Content(variableBlockSchema)

	varName := block.Labels[0]
	varType := cty.DynamicPseudoType
	varConstraintType := cty.DynamicPseudoType
	var varTypeDefaults *typeexpr.Defaults
	var defaultValue interface{}
	var description string
	var sensitive bool

	// Extract type constraint.
	if attr, exists := content.Attributes["type"]; exists {
		ty, tyDefaults, diags := typeexpr.TypeConstraintWithDefaults(attr.Expr)
		if diags.HasErrors() {
			return "", provisionings.TemplateOption{}, errors.Wrapf(
				diags, "type constraint on var %s", varName,
			)
		}
		varType = ty.WithoutOptionalAttributesDeep()
		varConstraintType = ty
		varTypeDefaults = tyDefaults
	}

	// Extract default value.
	if attr, exists := content.Attributes["default"]; exists {
		val, valDiags := attr.Expr.Value(nil)
		if valDiags.HasErrors() {
			return "", provisionings.TemplateOption{}, errors.Wrapf(
				valDiags, "default value on var %s", varName,
			)
		}

		if varConstraintType != cty.NilType {
			if varTypeDefaults != nil && !val.IsNull() {
				val = varTypeDefaults.Apply(val)
			}
			var convertErr error
			val, convertErr = convert.Convert(val, varConstraintType)
			if convertErr != nil {
				return "", provisionings.TemplateOption{}, errors.Wrapf(
					convertErr, "convert default value on var %s", varName,
				)
			}

			valJSON, marshalErr := ctyjson.Marshal(val, val.Type())
			if marshalErr != nil {
				return "", provisionings.TemplateOption{}, errors.Wrapf(
					marshalErr, "marshal default value on var %s", varName,
				)
			}

			var def interface{}
			if unmarshalErr := json.Unmarshal(valJSON, &def); unmarshalErr != nil {
				return "", provisionings.TemplateOption{}, errors.Wrapf(
					unmarshalErr, "unmarshal default value on var %s", varName,
				)
			}
			defaultValue = def
		}
	}

	// Extract description.
	if attr, exists := content.Attributes["description"]; exists {
		val, diags := attr.Expr.Value(nil)
		if !diags.HasErrors() && val.Type() == cty.String {
			description = val.AsString()
		}
	}

	// Extract sensitive.
	if attr, exists := content.Attributes["sensitive"]; exists {
		val, diags := attr.Expr.Value(nil)
		if !diags.HasErrors() && val.Type() == cty.Bool {
			sensitive = val.True()
		}
	}

	opt, err := decomposeType(varType, defaultValue)
	if err != nil {
		return "", provisionings.TemplateOption{}, errors.Wrapf(
			err, "decompose type on var %s", varName,
		)
	}
	opt.Required = defaultValue == nil && !existsAttribute(content, "default")
	opt.Description = description
	opt.Sensitive = sensitive
	// Preserve the full HCL type constraint string (with optional() wrappers)
	// for display purposes. Use varConstraintType so users can see the full
	// schema including defaults embedded in optional() markers.
	if varConstraintType != cty.DynamicPseudoType {
		opt.FullType = typeexpr.TypeString(varConstraintType)
	}

	return varName, opt, nil
}

// existsAttribute checks whether an attribute was declared in the block
// content (even if its value is nil/null).
func existsAttribute(content *hcl.BodyContent, name string) bool {
	_, exists := content.Attributes[name]
	return exists
}

// decomposeType recursively converts a cty.Type and its default value into a
// TemplateOption tree.
func decomposeType(
	variable cty.Type, defaultValue interface{},
) (provisionings.TemplateOption, error) {
	opt := provisionings.TemplateOption{
		Type:  typeexpr.TypeString(variable),
		Value: defaultValue,
	}

	switch {
	case variable.IsMapType():
		opt.Type = "map"
		values := make(map[string]provisionings.TemplateOption)

		var mapDefault map[string]interface{}
		switch t := defaultValue.(type) {
		case map[string]interface{}:
			mapDefault = t
		case nil:
			mapDefault = make(map[string]interface{})
		default:
			return opt, fmt.Errorf(
				"expected map[string]interface{} for map default, got %T", t,
			)
		}

		for key, val := range mapDefault {
			subOpt, err := decomposeType(variable.ElementType(), val)
			if err != nil {
				return opt, errors.Wrapf(err, "map key %s", key)
			}
			values[key] = subOpt
		}
		opt.Value = values

	case variable.IsObjectType():
		opt.Type = "object"
		values := make(map[string]provisionings.TemplateOption)

		var mapDefault map[string]interface{}
		switch t := defaultValue.(type) {
		case map[string]interface{}:
			mapDefault = t
		case nil:
			mapDefault = make(map[string]interface{})
		default:
			return opt, fmt.Errorf(
				"expected map[string]interface{} for object default, got %T", t,
			)
		}

		for attrName, attrType := range variable.AttributeTypes() {
			var defaultVal interface{}
			if v, ok := mapDefault[attrName]; ok {
				defaultVal = v
			}
			subOpt, err := decomposeType(attrType, defaultVal)
			if err != nil {
				return opt, errors.Wrapf(err, "object attr %s", attrName)
			}
			values[attrName] = subOpt
		}
		opt.Value = values

	case variable.IsListType(), variable.IsSetType():
		if variable.IsSetType() {
			opt.Type = "set"
		} else {
			opt.Type = "list"
		}

		// Inner type metadata (element type schema without values).
		innerType, err := decomposeType(variable.ElementType(), nil)
		if err != nil {
			return opt, errors.Wrap(err, "inner type for list/set")
		}
		opt.InnerTypes = []provisionings.TemplateOption{innerType}

		// Decompose actual default values.
		var list []interface{}
		switch t := defaultValue.(type) {
		case []interface{}:
			list = t
		case nil:
			list = make([]interface{}, 0)
		default:
			return opt, fmt.Errorf(
				"expected []interface{} for %s default, got %T", opt.Type, t,
			)
		}

		values := make([]provisionings.TemplateOption, 0, len(list))
		for _, elem := range list {
			subOpt, err := decomposeType(variable.ElementType(), elem)
			if err != nil {
				return opt, errors.Wrap(err, "list/set element")
			}
			values = append(values, subOpt)
		}
		opt.Value = values

	case variable.IsTupleType():
		opt.Type = "tuple"
		opt.InnerTypes = make([]provisionings.TemplateOption, 0)
		values := make([]provisionings.TemplateOption, 0)

		var listDefault []interface{}
		switch t := defaultValue.(type) {
		case []interface{}:
			listDefault = t
		case nil:
			listDefault = make([]interface{}, 0)
		default:
			return opt, fmt.Errorf(
				"expected []interface{} for tuple default, got %T", t,
			)
		}

		for i, elemType := range variable.TupleElementTypes() {
			innerType, err := decomposeType(elemType, nil)
			if err != nil {
				return opt, errors.Wrapf(err, "tuple inner type at position %d", i)
			}
			opt.InnerTypes = append(opt.InnerTypes, innerType)

			var defaultVal interface{}
			if i < len(listDefault) {
				defaultVal = listDefault[i]
			}
			subOpt, err := decomposeType(elemType, defaultVal)
			if err != nil {
				return opt, errors.Wrapf(err, "tuple element at position %d", i)
			}
			values = append(values, subOpt)
		}
		opt.Value = values

	default:
		// Primitive types: normalize value to match the declared type.
		if opt.Value != nil {
			switch opt.Type {
			case "string":
				switch t := opt.Value.(type) {
				case string:
					// already correct
				case float64:
					opt.Value = fmt.Sprintf("%f", t)
				case bool:
					opt.Value = fmt.Sprintf("%t", t)
				}
			case "number":
				switch t := opt.Value.(type) {
				case string:
					f, err := strconv.ParseFloat(t, 64)
					if err != nil {
						return opt, errors.Wrapf(err, "convert string %q to number", t)
					}
					opt.Value = f
				case float64:
					// already correct
				}
			case "bool":
				switch t := opt.Value.(type) {
				case string:
					opt.Value = t == "true"
				case bool:
					// already correct
				}
			}
		}
	}

	return opt, nil
}
