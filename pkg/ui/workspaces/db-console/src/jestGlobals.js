// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/* eslint-env node */
/* global globalThis */

// Polyfill TextEncoder/TextDecoder for jsdom test environment.
// Required by @segment/analytics-node's transitive dependency on jose.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { TextEncoder, TextDecoder } = require("util");
globalThis.TextEncoder = TextEncoder;
globalThis.TextDecoder = TextDecoder;
