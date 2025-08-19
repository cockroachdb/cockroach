// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/util/buildutil"

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
	if (len(*ann) < int(n.AnnIdx) || int(n.AnnIdx) < 1) && !buildutil.CrdbTestBuild {
		// Avoid index-out-of bounds panics in production builds. The impact is
		// that the node will be treated as if it doesn't have the annotation,
		// which is better than the process crash.
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
