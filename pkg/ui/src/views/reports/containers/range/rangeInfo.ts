// Copyright 2018 The Cockroach Authors.
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

import _ from "lodash";
import Long from "long";

import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";

export function GetLocalReplica(info: protos.cockroach.server.serverpb.IRangeInfo) {
  return _.find(info.state.state.desc.replicas, rep => rep.store_id === info.source_store_id);
}

export function IsLeader(info: protos.cockroach.server.serverpb.IRangeInfo) {
  const localRep = GetLocalReplica(info);
  if (_.isNil(localRep)) {
    return false;
  }
  return Long.fromInt(localRep.replica_id).eq(FixLong(info.raft_state.lead));
}

export default {
  GetLocalReplica: GetLocalReplica,
  IsLeader: IsLeader,
};
