// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdcevent

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
)

// DefaultCacheConfig is the default configuration for unordered cache.
var DefaultCacheConfig = cache.Config{
	Policy: cache.CacheFIFO,
	// TODO: If we find ourselves thrashing here in changefeeds on many tables,
	// we can improve performance by eagerly evicting versions using Resolved notifications.
	// A old Version with a timestamp entirely before a notification can be safely evicted.
	ShouldEvict: func(size int, _ interface{}, _ interface{}) bool { return size > 1024 },
}

// CacheKey is the key for the event caches.
type CacheKey struct {
	ID       descpb.ID
	Version  descpb.DescriptorVersion
	FamilyID descpb.FamilyID
}
