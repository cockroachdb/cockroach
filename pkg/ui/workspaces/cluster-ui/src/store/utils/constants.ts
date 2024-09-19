// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Default value for data invalidation in redux store.
// The main purpose is to decrease the number of requests on server.
export const CACHE_INVALIDATION_PERIOD = 300000; // defaults to 5min
export const DOMAIN_NAME = "adminUI";
