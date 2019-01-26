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

package tree

// AnnotationIdx is the 1-based index of an annotation. AST nodes that can
// be annotated store such an index (unique within that AST).
type AnnotationIdx int32

// NoAnnotation is the uninitialized annotation index.
const NoAnnotation AnnotationIdx = 0

// AnnotatedNode is embedded in AST nodes that have an annotation.
type AnnotatedNode struct {
	AnnIdx AnnotationIdx
}

// GetAnnotation retrieves the annotation associated with this node.
func (n AnnotatedNode) GetAnnotation(ann *Annotations) interface{} {
	if n.AnnIdx == NoAnnotation {
		return nil
	}
	return ann.Get(n.AnnIdx)
}

// SetAnnotation sets the annotation associated with this node.
func (n AnnotatedNode) SetAnnotation(ann *Annotations, annotation interface{}) {
	ann.Set(n.AnnIdx, annotation)
}

// Annotations is a container for AST annotations.
type Annotations []interface{}

// MakeAnnotations allocates an annotations container of the given size.
func MakeAnnotations(numAnnotations AnnotationIdx) Annotations {
	return make(Annotations, numAnnotations)
}

// Set an annotation in the container.
func (a *Annotations) Set(idx AnnotationIdx, annotation interface{}) {
	(*a)[idx-1] = annotation
}

// Get an annotation from the container.
func (a *Annotations) Get(idx AnnotationIdx) interface{} {
	return (*a)[idx-1]
}
