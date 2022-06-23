// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package buflintv1 contains the VersionSpec for v1.
//
// It uses buflintcheck and buflintbuild.
//
// The only changes from v1beta1 to v1 were that ENUM_FIRST_VALUE_ZERO was moved
// from OTHER to MINIMAL, and the OTHER category was deleted.
package buflintv1

import "github.com/bufbuild/buf/private/buf/bufcheck/internal"

// VersionSpec is the version specification for v1.
//
// The SYNTAX_SPECIFIED rule was added to BASIC, DEFAULT.
// The IMPORT_USED rule was added to BASIC, DEFAULT.
//
// A number of categories were removed between v1beta1 and v1. The difference
// is shown below:
//
// Removed:
//  * FILE_LAYOUT
//  * PACKAGE_AFFINITY
//  * SENSIBLE
//  * STYLE_BASIC
//  * STYLE_DEFAULT
//  * OTHER
//
// Thus, the only remaining categories are:
//
// Categories:
//  * MINIMAL
//  * BASIC
//  * DEFAULT
//  * COMMENTS
//  * UNARY_RPC
//
// The rules included in the MINIMAL lint category have also been adjusted.
// The difference is shown below:
//
// Removed:
//  * ENUM_NO_ALLOW_ALIAS
//  * FIELD_NO_DESCRIPTOR
//  * IMPORT_NO_PUBLIC
//  * IMPORT_NO_WEAK
//  * PACKAGE_SAME_CSHARP_NAMESPACE
//  * PACKAGE_SAME_GO_PACKAGE
//  * PACKAGE_SAME_JAVA_MULTIPLE_FILES
//  * PACKAGE_SAME_JAVA_PACKAGE
//  * PACKAGE_SAME_PHP_NAMESPACE
//  * PACKAGE_SAME_RUBY_PACKAGE
//  * PACKAGE_SAME_SWIFT_PREFIX
//
// With these changes applied, the final result of MINIMAL is:
//
// MINIMAL:
//  * DIRECTORY_SAME_PACKAGE
//  * PACKAGE_DEFINED
//  * PACKAGE_DIRECTORY_MATCH
//  * PACKAGE_SAME_DIRECTORY
//
// Finally, the FIELD_NO_DESCRIPTOR rule was removed altogether.
var VersionSpec = &internal.VersionSpec{
	RuleBuilders:      v1RuleBuilders,
	DefaultCategories: v1DefaultCategories,
	AllCategories:     v1AllCategories,
	IDToCategories:    v1IDToCategories,
}
