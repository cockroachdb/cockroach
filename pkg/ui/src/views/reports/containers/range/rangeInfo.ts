import _ from "lodash";
import Long from "long";

import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";

export function GetLocalReplica(info: protos.cockroach.server.serverpb.RangeInfo$Properties) {
  return _.find(info.state.state.desc.replicas, rep => rep.store_id === info.source_store_id);
}

export function IsLeader(info: protos.cockroach.server.serverpb.RangeInfo$Properties) {
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
