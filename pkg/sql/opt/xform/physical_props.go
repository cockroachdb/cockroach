// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package xform

// physicalPropsID identifies a set of physical properties that has been
// interned by a memo instance. If two ids are different, then the physical
// properties are different.
type physicalPropsID uint32

const (
	// minPhysPropsID is the id of the well-known set of physical properties
	// that requires nothing of an operator. Therefore, every operator is
	// guaranteed to provide this set of properties.
	minPhysPropsID physicalPropsID = 1
)

// PhysicalProps are interesting characteristics of an expression that impact
// its layout, presentation, or location, but not its logical content. For
// example, sort order will change the order of rows, but not the membership or
// data content of the rows.
//
// Physical properties can be provided by an operator or required of it. Some
// operators "naturally" provide a physical property such as ordering on a
// particular column. Other operators require one or more of their operands to
// provide a particular physical property. When an expression is optimized, it
// is always with respect to a particular set of required physical properties.
// The goal is to find the lowest cost expression that provides those
// properties while still remaining logically equivalent.
//
// TODO(andyk): add physical properties to the struct once we're ready for
// them.
type PhysicalProps struct {
}

// Make the linter happy until we start using this.
var _ = PhysicalProps{}
