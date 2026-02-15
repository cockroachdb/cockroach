// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"reflect"
	"strings"

	"github.com/cockroachdb/errors"
)

// FieldMapper handles reflection-based SCIM field mapping.
// It extracts SCIM field names from struct tags and builds a bidirectional
// mapping between SCIM field names and internal database field names.
type FieldMapper struct {
	scimToInternal map[string]string   // SCIM field -> internal field
	internalToScim map[string][]string // internal field -> SCIM fields
}

// NewFieldMapper creates a new field mapper.
func NewFieldMapper() *FieldMapper {
	return &FieldMapper{
		scimToInternal: make(map[string]string),
		internalToScim: make(map[string][]string),
	}
}

// BuildMapping generates field mapping from struct tags using reflection.
// It extracts both the 'scim' tag (for SCIM field names) and 'db' tag
// (for internal database field names) from the provided struct type.
//
// The 'scim' tag can contain multiple comma-separated SCIM field names
// that all map to the same internal field. For example:
//
//	Email string `db:"email" scim:"userName,emails"`
//
// This creates two mappings: userName->email and emails->email
func (fm *FieldMapper) BuildMapping(structType reflect.Type) error {
	// Clear existing mappings
	fm.scimToInternal = make(map[string]string)
	fm.internalToScim = make(map[string][]string)

	// Validate input type
	if structType.Kind() != reflect.Struct {
		return errors.Newf("expected struct type, got %s", structType.Kind())
	}

	// Iterate through struct fields
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// Get SCIM tag
		scimTag := field.Tag.Get("scim")
		if scimTag == "" || scimTag == "-" {
			continue // Skip fields without SCIM mapping
		}

		// Get internal field name from db tag (snake_case database column name)
		dbTag := field.Tag.Get("db")
		if dbTag == "" || dbTag == "-" {
			continue // Skip fields without db mapping
		}

		// Handle multiple SCIM field names (comma-separated)
		scimFields := strings.Split(scimTag, ",")
		for _, scimField := range scimFields {
			scimField = strings.TrimSpace(scimField)
			if scimField == "" {
				continue
			}

			// Map SCIM field to struct field name (for reflection-based query builders)
			// The SQL builder uses FieldByName() which requires the Go struct field name,
			// then reads the db tag to get the actual column name.
			fm.scimToInternal[scimField] = field.Name

			// Map internal field to SCIM fields (reverse mapping)
			fm.internalToScim[field.Name] = append(fm.internalToScim[field.Name], scimField)
		}
	}

	return nil
}

// GetInternalField returns the internal field name for a SCIM field.
// Returns (fieldName, true) if found, ("", false) otherwise.
func (fm *FieldMapper) GetInternalField(scimField string) (string, bool) {
	internal, ok := fm.scimToInternal[scimField]
	return internal, ok
}

// GetSCIMFields returns the SCIM field names for an internal field.
// Returns ([]string, true) if found, (nil, false) otherwise.
func (fm *FieldMapper) GetSCIMFields(internalField string) ([]string, bool) {
	scimFields, ok := fm.internalToScim[internalField]
	return scimFields, ok
}

// ToMap returns the SCIM->Internal mapping as a map.
// This creates a copy to prevent external modification of internal state.
func (fm *FieldMapper) ToMap() map[string]string {
	result := make(map[string]string, len(fm.scimToInternal))
	for k, v := range fm.scimToInternal {
		result[k] = v
	}
	return result
}
