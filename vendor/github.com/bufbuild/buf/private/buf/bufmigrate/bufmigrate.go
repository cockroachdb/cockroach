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

// Package bufmigrate contains logic for migrating between different
// configuration file versions.
package bufmigrate

// Migrator describes the interface used to migrate
// a set of files in a directory from one version to another.
type Migrator interface {
	Migrate(dirPath string) error
}

// V1Beta1MigrateOption defines the type used
// to configure the v1beta1 migrator.
type V1Beta1MigrateOption func(*v1beta1Migrator)

// NewV1Beta1Migrator creates a new migrator that migrates files
// between version v1beta1 and v1.
func NewV1Beta1Migrator(commandName string, options ...V1Beta1MigrateOption) Migrator {
	return newV1Beta1Migrator(commandName, options...)
}

// V1Beta1MigratorWithNotifier instruments the migrator with
// a callback to call whenever an event that should notify the
// user occurs during the migration.
func V1Beta1MigratorWithNotifier(notifier func(message string) error) V1Beta1MigrateOption {
	return func(migrateOptions *v1beta1Migrator) {
		migrateOptions.notifier = notifier
	}
}
