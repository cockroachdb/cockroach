// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package gossip implements a protocol for sharing information between
Cockroach nodes using an ad-hoc, peer-to-peer network. The
self-assembled network aims to minimize time for new information to
reach each node, and minimize network traffic required.

Gossiped information is identified by key. Gossip information is
captured by info objects.

Single-valued info values can have any type.

A map of info objects is kept by a Gossip instance. Single-valued info
objects can be added via Gossip.AddInfo(). Info can be queried for
single-valued keys via Gossip.GetInfo.
*/
package gossip
