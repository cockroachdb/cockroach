// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// NumStates is the number of values which State may take on.
var NumStates = len(State_name)

// Node represents a Target in a given state.
type Node struct {
	Target *Target
	State  State
}

// Element returns the target's element.
func (n *Node) Element() Element {
	return n.Target.Element()
}

// FingerprintKey are keys used for finger prints of objects
// for comparing uniqueness
type FingerprintKey string

// Fingerprint is a map of key value pairs representing key
// values that make an element unique.
type Fingerprint map[FingerprintKey]interface{}

const (
	//FingerprintColumnID column ID
	FingerprintColumnID FingerprintKey = "ColumnID"
	// FingerprintElementName name of the element
	FingerprintElementName FingerprintKey = "ElementName"
	// FingerprintIndexID index ID
	FingerprintIndexID FingerprintKey = "IndexID"
	// FingerprintDescID main descriptor ID
	FingerprintDescID FingerprintKey = "DescID"
	// FingerprintDepID dependent descriptor ID
	FingerprintDepID FingerprintKey = "DepDescID"
	// FingerprintType type id of the element
	FingerprintType FingerprintKey = "Type"
)

// Element represents a logical component of a catalog entry's schema (e.g., an
// index or column in a table).
type Element interface {
	protoutil.Message
	DescriptorID() descpb.ID
	Fingerprint() Fingerprint
}

// Element returns an Element from its wrapper for serialization.
func (e *ElementProto) Element() Element {
	return e.GetValue().(Element)
}

// NewTarget constructs a new Target. The passed elem must be one of the oneOf
// members of Element. If not, this call will panic.
func NewTarget(dir Target_Direction, elem Element) *Target {
	t := Target{
		Direction: dir,
	}
	if !t.SetValue(elem) {
		panic(errors.Errorf("unknown element type %T", elem))
	}
	return &t
}

// ElementTypeID represents type ID of a element
type ElementTypeID int

var typeToElementID map[reflect.Type]ElementTypeID

func init() {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	typeToElementID = make(map[reflect.Type]ElementTypeID, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		protoFlags := strings.Split(f.Tag.Get("protobuf"), ",")
		id, err := strconv.Atoi(protoFlags[1])
		if err != nil {
			panic(errors.Wrapf(err, "failed to extract ID from protobuf tag: %q", protoFlags))
		}
		typeToElementID[f.Type] = ElementTypeID(id)
	}
}

// ElementType determines the type ID of a element
func ElementType(el Element) ElementTypeID {
	return typeToElementID[reflect.TypeOf(el)]
}

// DescriptorID implements the Element interface.
func (e *Column) DescriptorID() descpb.ID { return e.TableID }

// Fingerprint implements the Element interface
func (e *Column) Fingerprint() Fingerprint {
	return Fingerprint{
		FingerprintColumnID:    e.Column.ID,
		FingerprintDescID:      e.TableID,
		FingerprintElementName: e.Column.Name,
		FingerprintType:        ElementType(e),
	}
}

// DescriptorID implements the Element interface.
func (e *PrimaryIndex) DescriptorID() descpb.ID { return e.TableID }

// Fingerprint implements the Element interface
func (e *PrimaryIndex) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintIndexID:     e.Index.ID,
		FingerprintDescID:      e.TableID,
		FingerprintElementName: e.Index.Name,
		FingerprintType:        ElementType(e),
	}
}

// DescriptorID implements the Element interface.
func (e *SecondaryIndex) DescriptorID() descpb.ID { return e.TableID }

// Fingerprint implements the Element interface
func (e *SecondaryIndex) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintIndexID:     e.Index.ID,
		FingerprintDescID:      e.TableID,
		FingerprintElementName: e.Index.Name,
		FingerprintType:        ElementType(e),
	}
}

// DescriptorID implements the Element interface.
func (e *SequenceDependency) DescriptorID() descpb.ID { return e.SequenceID }

// Fingerprint implements the Element interface
func (e *SequenceDependency) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintColumnID: e.ColumnID,
		FingerprintDepID:    e.TableID,
		FingerprintDescID:   e.SequenceID,
		FingerprintType:     ElementType(e),
	}
}

// DescriptorID implements the Element interface.
func (e *UniqueConstraint) DescriptorID() descpb.ID { return e.TableID }

// Fingerprint implements the Element interface
func (e *UniqueConstraint) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintIndexID: e.IndexID,
		FingerprintDescID:  e.TableID,
		FingerprintType:    ElementType(e),
	}
}

// DescriptorID implements the Element interface.
func (e *CheckConstraint) DescriptorID() descpb.ID { return e.TableID }

// Fingerprint implements the Element interface
func (e *CheckConstraint) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintDescID:      e.TableID,
		FingerprintElementName: e.Name,
		FingerprintType:        ElementType(e),
	}
}

// DescriptorID implements the Element interface.
func (e *Sequence) DescriptorID() descpb.ID { return e.TableID }

// Fingerprint implements the Element interface
func (e *Sequence) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintDescID: e.TableID,
		FingerprintType:   ElementType(e),
	}
}

// DescriptorID implements the Element interface.
func (e *DefaultExpression) DescriptorID() descpb.ID { return e.TableID }

// Fingerprint implements the Element interface
func (e *DefaultExpression) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintColumnID: e.ColumnID,
		FingerprintDescID:   e.TableID,
		FingerprintType:     ElementType(e),
	}
}

// DescriptorID implements the Element interface.
func (e *View) DescriptorID() descpb.ID { return e.TableID }

// Fingerprint implements the Element interface
func (e *View) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintDescID: e.TableID,
		FingerprintType:   ElementType(e),
	}
}

// DescriptorID implements the Element interface
func (e *TypeReference) DescriptorID() descpb.ID { return e.DescID }

// Fingerprint implements the Element interface
func (e *TypeReference) Fingerprint() Fingerprint {
	return map[FingerprintKey]interface{}{
		FingerprintDepID:  e.DescID,
		FingerprintDescID: e.TypeID,
		FingerprintType:   ElementType(e),
	}
}
