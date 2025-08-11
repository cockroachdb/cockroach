// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil/serde"
	"gopkg.in/yaml.v3"
)

type (
	stepTypes struct {
		SingleStep        singleStep `type:"pointer"`
		SequentialRunStep sequentialRunStep
		DelayedStep       delayedStep
		ConcurrentRunStep concurrentRunStep
	}
	stepTypeWrapper serde.TypeWrapper

	dynamicStep interface {
		getTypeName(*stepTypes) (string, error)
	}
)

var stepTypesMap = (&stepTypes{}).toTypeMap()

func (t *stepTypes) toTypeMap() map[string]reflect.StructField {
	return serde.ToTypeMap(t)
}

func (t *stepTypes) getTypeName(
	dynamicType dynamicStep, validationRef dynamicStep,
) (string, error) {
	return serde.GetTypeName(t, dynamicType, validationRef)
}

func (t testStep) MarshalYAML() (any, error) {
	typeName, err := t.Val.getTypeName(&stepTypes{})
	if err != nil {
		return nil, fmt.Errorf("failed to get type name for %T: %w", t.Val, err)
	}
	return stepTypeWrapper{
		Type: typeName,
		Val:  t.Val,
	}, nil
}

func (t *testStep) UnmarshalYAML(value *yaml.Node) error {
	var w stepTypeWrapper
	if err := value.Decode(&w); err != nil {
		return fmt.Errorf("failed to decode testStep: %w", err)
	}
	*t = testStep{
		Val: w.Val.(dynamicStep),
	}
	return nil
}

func (t *stepTypeWrapper) UnmarshalYAML(value *yaml.Node) error {
	return (*serde.TypeWrapper)(t).UnmarshalYAML(stepTypesMap, value)
}
