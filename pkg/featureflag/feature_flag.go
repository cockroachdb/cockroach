// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package featureflag

import "github.com/cockroachdb/cockroach/pkg/settings"

// FeatureBackupEnabled is used to enable and disable the BACKUP feature.
var FeatureBackupEnabled = settings.RegisterPublicBoolSetting(
	"feature.backup.enabled",
	"set to true to enable backups, false to disable; default is true",
	true /* enabled by default */)
