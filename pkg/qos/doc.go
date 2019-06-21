// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package qos defines quality of service, a best-effort mechanism to prioritize
// traffic in cockroachdb.
//
// The quality of service Level is a request annotation which propagates through
// the system generally via a context mechanism and gRPC middleware. Components
// consult qos levels as part of providing mechanisms to mitigate overload and
// protect higher Class traffic from the load due to lower Class traffic. Class
// represents client intention to prioritize or deprioritize a request relative
// to other traffic. Shard is a uniform property of the client connection from
// which a Level originates.
//
// Shard helps the distributed system to mitigcate the challenges due to
// "multiple overload" whereby the processing of a single client request may
// require visiting more than downstream server which is overloaded. If traffic
// of a given Class were uniformly affected then even if each individual server
// were to only backpressure a small fraction of incident requests, a large
// fraction could observe some backpressure. Using a Shard in conjunction with a
// Class allows servers to agree on which traffic of a Class should experience
// backpressure without explicit coordination.
package qos
