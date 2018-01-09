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

package cluster

// LicenseType represents a type of CockroachDB license.
type LicenseType string

const (
	// LicenseTypeNone is the absence of a license.
	LicenseTypeNone LicenseType = "None"
	// LicenseTypeOSS indicates an open source CockroachDB build.
	LicenseTypeOSS LicenseType = "OSS"

	// The following license types correspond to the Type enum in license.proto.

	// LicenseTypeNonCommercial is a non-commercial license.
	LicenseTypeNonCommercial LicenseType = "NonCommercial"
	// LicenseTypeEnterprise is an enterprise license.
	LicenseTypeEnterprise LicenseType = "Enterprise"
	// LicenseTypeEvaluation is an evaluation license.
	LicenseTypeEvaluation LicenseType = "Evaluation"
)
