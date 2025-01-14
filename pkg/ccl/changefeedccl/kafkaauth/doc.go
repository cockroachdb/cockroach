// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kafkaauth provides Kafka SASL authentication mechanisms for use with
// the changefeed kafka sinks (v1 & v2). Most standard SASL mechanisms are
// supported, and some custom ones as well.
package kafkaauth
