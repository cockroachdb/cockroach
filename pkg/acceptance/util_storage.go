// Copyright 2015 The Cockroach Authors.
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

package acceptance

import "fmt"

// EphemeralStorageAccount returns the name of the storage account to use to
// store data that should be periodically purged. It returns a storage account
// in the region specified by the -tf.storage-location flag to avoid bandwidth
// egress charges.
//
// See /docs/CLOUD-RESOURCES.md for details.
func EphemeralStorageAccount() string {
	return "roachephemeral" + *flagTFStorageLocation
}

// FixtureStorageAccount returns the name of the storage account that contains
// permanent test data ("test fixtures"). It returns a storage account in the
// region specified by the -tf.storage-location flag to avoid bandwidth egress
// charges.
//
// See /docs/CLOUD-RESOURCES.md for details.
func FixtureStorageAccount() string {
	return "roachfixtures" + *flagTFStorageLocation
}

// FixtureURL returns the public URL at which the fixture with the given name
// can be downloaded from Azure Cloud Storage. Like FixtureStorageAccount(), it
// takes the -tf.storage-location flag into account.
func FixtureURL(name string) string {
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s", FixtureStorageAccount(), name)
}
