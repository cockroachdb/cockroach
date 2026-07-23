// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package spanconfigbounds provides tools to enforce bounds on spanconfigs.
//
// The logic here is used to apply the bounds stored in tenant capabilities
// to values which either exist in the span_configurations table or are being
// created via the zone configuration APIs. The KV layer will use Clamp() to
// ensure that it only realizes configurations the operator allows for that
// tenant. The Check() function can be used by the SQL layer to reject new
// configurations which are non-conforming.
//
// The logic for bounding numeric values is straight-forward. The behavior of
// ConstraintBounds in the case of non-conformance may be surprising. The goals
// are:
//
//  1. Ensure data does not leave the intended servers as indicated by
//     the operator.
//  2. Minimize surprise when the span config violates the bounds.
//  3. Do something coherent with the MR primitives.
//
// Point (3) pushes the behavior of these constraints to behave differently
// in response to the structure of the requested constraints.
//
// The operator configures a set of allowed constraints, and, additionally,
// will configure a set of fallbacks to control the behavior when the
// requested constraints do not conform.
package spanconfigbounds
