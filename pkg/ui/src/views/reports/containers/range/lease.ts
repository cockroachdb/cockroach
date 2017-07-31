import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";

export function IsLeaseEpoch(lease: protos.cockroach.roachpb.Lease$Properties) {
  return !FixLong(lease.epoch).eq(0);
}

export default {
  IsEpoch: IsLeaseEpoch,
};
