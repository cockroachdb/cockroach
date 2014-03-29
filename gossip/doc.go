// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*
Package gossip implements a protocol for sharing information between
Cockroach nodes using an ad-hoc, peer-to-peer network. The
self-assembled network aims to minimize time for new information to
reach each node, and minimize network traffic required.

Gossiped information is identified by key. Gossip information is
captured by info objects. Info objects may be stored individually
(e.g. the number of nodes in the system), or may be organized into
groups (e.g. multiple values of the same type from different
originators).

Groups organize info objects by key prefix. Groups come in two types:
MinGroup groups keep only the minimum values seen; MaxGroup groups
keep only the maximum values seen. An example is load or disk capacity
values for nodes. In a cluster with thousands of nodes, groups force
the gossip network to limit itself to only a portion of total data
volume (e.g. the 100 least loaded nodes or the 100 disks with most
unused capacity).

A map of info objects and a map of Group objects are kept by the
Gossip instance. New info objects should be created via
Gossip.AddInt64Info, Gossip.AddFloat64Info and Gossip.AddStringInfo.
Groups are registered via Gossip.RegisterGroup. Info objects are
added to groups automatically if their key shares the group's key
prefix. Info can be queried from the gossip network via the
Gossip.GetInt64Info, Gossip.GetFloat64Info and Gossip.GetStringInfo.
Sorted values for groups are queried via Gossip.GetGroupInt64Infos,
Gossip.GetGroupFloat64Infos and Gossip.GetGroupStringInfos.
*/
package gossip
