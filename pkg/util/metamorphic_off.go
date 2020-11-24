// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !metamorphic

package util

// MetamorphicBuild is a flag that is set to true if the binary was compiled with
// the test_constants build tag. This flag can be used to enable expensive
// checks, test randomizations, or other metamorphic-style perturbations that
// will not affect test results but will exercise different parts of the code.
const MetamorphicBuild = false
