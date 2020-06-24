// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

// TODO (rohany): this could be renamed? Or possibly just moved out of this file.
type ObjectPrefix struct {
	// TODO (rohany): Should this be an ImmutableDatabaseDescriptor?
	Database DatabaseDescriptorInterface
	Schema   ResolvedSchema
}

// SchemaMeta implements the SchemaMeta interface.
func (*ObjectPrefix) SchemaMeta() {}
