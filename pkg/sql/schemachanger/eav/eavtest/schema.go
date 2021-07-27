// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eavtest

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/errors"
)

// Schema implements eav.Schema for testing.
type Schema struct {
	attributes []attribute
	byName     map[string]*attribute
	ords       eav.OrdinalSet

	onGetFunc func(eav.Attribute)
}

func (s Schema) onGet(a eav.Attribute) {
	if s.onGetFunc != nil {
		s.onGetFunc(a)
	}
}

// Attributes is part of eav.Schema.
func (s Schema) Attributes() eav.OrdinalSet { return s.ords }

// At is part of eav.Schema.
func (s Schema) At(i eav.Ordinal) eav.Attribute { return &s.attributes[i] }

// NewSchema constructs a new Schema.
func NewSchema(md []AttributeMetadata) *Schema {
	attributes := make([]attribute, len(md))
	byName := make(map[string]*attribute, len(md))
	var ords eav.OrdinalSet
	for i := range md {
		attributes[i] = attribute{
			AttributeMetadata: md[i],
			ord:               eav.Ordinal(i),
		}
		if existing, ok := byName[md[i].Name]; ok {
			panic(errors.AssertionFailedf("%d: attribute with name %s already exists with ordinal %d",
				i, md[i].Name, existing.Ordinal()))
		}
		byName[md[i].Name] = &attributes[i]
		ords = ords.Add(attributes[i].ord)
	}

	return &Schema{
		attributes: attributes,
		byName:     byName,
		ords:       ords,
	}
}

var _ eav.Schema = (*Schema)(nil)

func (s *Schema) parseAttributes(attrs []string) (ret []eav.Attribute, err error) {
	ret = make([]eav.Attribute, len(attrs))
	for i, name := range attrs {
		if ret[i], err = s.lookupAttribute(name); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (s *Schema) lookupAttribute(a string) (*attribute, error) {
	attr, ok := s.byName[a]
	if !ok {
		return nil, errors.Errorf("unknown attribute with name %s", a)
	}
	return attr, nil
}

func (s *Schema) parseValues(m map[string]interface{}) (eav.Values, error) {
	v := eav.GetValues()
	for name, value := range m {
		attr, err := s.lookupAttribute(name)
		if err != nil {
			return nil, err
		}
		val, err := attr.parseValue(value)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse attribute %s", name)
		}
		v.Set(attr, val)
	}
	return v, nil
}

func (s *Schema) setOnGet(f func(attribute2 eav.Attribute)) (cleanup func()) {
	s.onGetFunc = f
	return func() { s.onGetFunc = nil }
}
