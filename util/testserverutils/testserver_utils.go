// Copyright 2016 The Cockroach Authors.
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
//
// Author: Andrei Matei (andreimatei1@gmail.com)

package testserverutils

import (
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// TestStoresInterface is implemented by server.TestStores.
// It embeds a Stores and give convenient access to tests that can't depend on
// storage.
type TestStoresInterface interface {
	client.Sender
	GetStore(roachpb.StoreID) (TestStoreInterface, error)
	// GetStores returns the storage.Stores instance.
	GetStores() interface{}
}

// TestStoreInterface is implemented by *storage.Store.
type TestStoreInterface interface {
	// Engine accessor.
	// NB: This introduces a dependency on engine. That package is low-level enough
	// that it seems unlikely that a util or one of its clients can't depend on it.
	Engine() engine.Engine
}
