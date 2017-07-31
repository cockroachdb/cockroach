import _ from "lodash";
import Long from "long";
import moment from "moment";
import React from "react";

import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";
import { LongToMoment, NanoToMilli } from "src/util/convert";
import Lease from "src/views/reports/containers/range/lease";
import Print from "src/views/reports/containers/range/print";
import RangeInfo from "src/views/reports/containers/range/rangeInfo";

interface RangeTableProps {
  infos: protos.cockroach.server.serverpb.RangeInfo$Properties[];
  replicas: protos.cockroach.roachpb.ReplicaDescriptor$Properties[];
}

interface RangeTableRow {
  readonly variable: string;
  readonly display: string;
  readonly compareToLeader: boolean; // When true, display a warnings when a value doesn't match the
  // leader's.
}

interface RangeTableCellContent {
  value: string[];
  title?: string[];
  className?: string[];
}

interface RangeTableDetail {
  [name: string]: RangeTableCellContent;
}

const rangeTableDisplayList: RangeTableRow[] = [
  { variable: "id", display: "ID", compareToLeader: false },
  { variable: "keyRange", display: "Key Range", compareToLeader: true },
  { variable: "raftState", display: "Raft State", compareToLeader: false },
  { variable: "leaseHolder", display: "Lease Holder", compareToLeader: true },
  { variable: "leaseType", display: "Lease Type", compareToLeader: true },
  { variable: "leaseEpoch", display: "Lease Epoch", compareToLeader: true },
  { variable: "leaseStart", display: "Lease Start", compareToLeader: true },
  { variable: "leaseExpiration", display: "Lease Expiration", compareToLeader: true },
  { variable: "leaseAppliedIndex", display: "Lease Applied Index", compareToLeader: true },
  { variable: "raftLeader", display: "Raft Leader", compareToLeader: true },
  { variable: "vote", display: "Vote", compareToLeader: false },
  { variable: "term", display: "Term", compareToLeader: true },
  { variable: "applied", display: "Applied", compareToLeader: true },
  { variable: "commit", display: "Commit", compareToLeader: true },
  { variable: "lastIndex", display: "Last Index", compareToLeader: true },
  { variable: "logSize", display: "Log Size", compareToLeader: false },
  { variable: "leaseHolderQPS", display: "Lease Holder QPS", compareToLeader: false },
  { variable: "keysWrittenPS", display: "Keys Written Per Second", compareToLeader: false },
  { variable: "approxProposalQuota", display: "Approx Proposal Quota", compareToLeader: false },
  { variable: "pendingCommands", display: "Pending Commands", compareToLeader: false },
  { variable: "droppedCommands", display: "Dropped Commands", compareToLeader: false },
  { variable: "truncatedIndex", display: "Truncated Index", compareToLeader: true },
  { variable: "truncatedTerm", display: "Truncated Term", compareToLeader: true },
  { variable: "mvccLastUpdate", display: "MVCC Last Update", compareToLeader: true },
  { variable: "mvccIntentAge", display: "MVCC Intent Age", compareToLeader: true },
  { variable: "mvccGGBytesAge", display: "MVCC GG Bytes Age", compareToLeader: true },
  { variable: "mvccLiveBytesCount", display: "MVCC Live Bytes/Count", compareToLeader: true },
  { variable: "mvccKeyBytesCount", display: "MVCC Key Bytes/Count", compareToLeader: true },
  { variable: "mvccValueBytesCount", display: "MVCC Value Bytes/Count", compareToLeader: true },
  { variable: "mvccIntentBytesCount", display: "MVCC Intent Bytes/Count", compareToLeader: true },
  { variable: "mvccSystemBytesCount", display: "MVCC System Bytes/Count", compareToLeader: true },
];

const rangeTableEmptyContent: RangeTableCellContent = {
  value: ["-"],
};

export default class RangeTable extends React.Component<RangeTableProps, {}> {
  cleanRaftState(state: string) {
    switch (_.toLower(state)) {
      case "statedormant": return "dormant";
      case "stateleader": return "leader";
      case "statefollower": return "follower";
      case "statecandidate": return "candidate";
      case "stateprecandidate": return "precandidate";
      default: return "unknown";
    }
  }

  contentRaftState(state: string): RangeTableCellContent {
    const cleanedState = this.cleanRaftState(state);
    return {
      value: [cleanedState],
      className: [`range-table__cell--raftstate-${cleanedState}`],
    };
  }

  contentNanos(nanos: Long): RangeTableCellContent {
    const humanized = Print.Time(LongToMoment(nanos));
    return {
      value: [humanized],
      title: [humanized, nanos.toString()],
    };
  }

  contentDuration(nanos: Long): RangeTableCellContent {
    const humanized = Print.Duration(moment.duration(NanoToMilli(nanos.toNumber())));
    return {
      value: [humanized],
      title: [humanized, nanos.toString()],
    };
  }

  contentMVCC(bytes: Long, count: Long): RangeTableCellContent {
    return this.createContent(`${bytes.toString()} bytes / ${count.toString()}`);
  }

  createContent(value: string | Long | number, className: string = null): RangeTableCellContent {
    if (_.isNull(className)) {
      return {
        value: [value.toString()],
      };
    }
    return {
      value: [value.toString()],
      className: [className],
    };
  }

  contentTimestamp(timestamp: protos.cockroach.util.hlc.Timestamp$Properties): RangeTableCellContent {
    const humanized = Print.Timestamp(timestamp);
    return {
      value: [humanized],
      title: [humanized, timestamp.wall_time.toString()],
    };
  }

  renderRangeCell(
    row: RangeTableRow,
    cell: RangeTableCellContent,
    key: number,
    dormant: boolean,
    leaderCell?: RangeTableCellContent,
  ) {
    const title = _.join(_.isNil(cell.title) ? cell.value : cell.title, "\n");
    const classNames = _.concat(cell.className, ["range-table__cell"]);
    if (dormant) {
      classNames.push("range-table__cell--dormant");
    } else {
      if (!_.isNil(leaderCell) && row.compareToLeader && !_.isEqual(cell.value, leaderCell.value)) {
        classNames.push("range-table__cell--different-from-leader-warning");
      }
    }
    const className = _.join(classNames, " ");
    return (
      <td key={key} className={className} title={title}>
        <ul className="range-entries-list">
          {
            _.map(cell.value, (value, k) => (
              <li key={k}>
                {value}
              </li>
            ))
          }
        </ul>
      </td>
    );
  }

  renderRangeRow(
    row: RangeTableRow,
    detailsByStoreID: Map<number, RangeTableDetail>,
    dormantStoreIDs: Set<number>,
    leaderStoreID: number,
    sortedStoreIDs: number[],
    key: number,
  ) {
    let headerClassName = "range-table__cell range-table__cell--header";
    const leaderDetail = detailsByStoreID.get(leaderStoreID);
    if (row.compareToLeader) {
      const values: Set<string> = new Set();
      detailsByStoreID.forEach((detail, storeID) => {
        if (!dormantStoreIDs.has(storeID)) {
          values.add(_.join(detail[row.variable].value, " "));
        }
      });
      if (values.size > 1) {
        headerClassName += " range-table__cell--header-warning";
      }
    }
    return (
      <tr key={key} className="range-table__row">
        <th className={headerClassName}>
          {row.display}
        </th>
        {
          _.map(sortedStoreIDs, (storeID) => {
            const cell = detailsByStoreID.get(storeID)[row.variable];
            const leaderCell = (storeID === leaderStoreID) ? null : leaderDetail[row.variable];
            return (
              this.renderRangeCell(
                row,
                cell,
                storeID,
                dormantStoreIDs.has(storeID),
                leaderCell,
              )
            );
          })
        }
      </tr>
    );
  }

  renderRangeReplicaCell(
    leaderReplicaIDs: Set<number>,
    replicaID: number,
    replica: protos.cockroach.roachpb.ReplicaDescriptor$Properties,
    rangeID: Long,
    localStoreID: number,
    dormant: boolean,
  ) {
    let className = "range-table__cell";
    if (_.isNil(replica)) {
      if (dormant) {
        className += " range-table__cell--dormant";
      } else if (leaderReplicaIDs.has(replicaID)) {
        className += " range-table__cell--different-from-leader-warning";
      }
      return (
        <td key={localStoreID} className={className}>
          -
        </td>
      );
    }
    if (dormant) {
      className += " range-table__cell--dormant";
    } else if (!leaderReplicaIDs.has(replica.replica_id)) {
      className += " range-table__cell--different-from-leader-warning";
    } else if (replica.store_id === localStoreID) {
      className += " range-table__cell--local-replica";
    }
    const value = Print.ReplicaID(rangeID, replica);
    return (
      <td key={localStoreID} className={className} title={value}>
        {value}
      </td>
    );
  }

  renderRangeReplicaRow(
    replicasByReplicaIDByStoreID: Map<number, Map<number, protos.cockroach.roachpb.ReplicaDescriptor$Properties>>,
    referenceReplica: protos.cockroach.roachpb.ReplicaDescriptor$Properties,
    leaderReplicaIDs: Set<number>,
    dormantStoreIDs: Set<number>,
    sortedStoreIDs: number[],
    rangeID: Long,
    key: number,
  ) {
    const headerClassName = "range-table__cell range-table__cell--header";
    return (
      <tr key={key} className="range-table__row">
        <th className={headerClassName}>
          Replica {referenceReplica.replica_id} - ({Print.ReplicaID(rangeID, referenceReplica)})
        </th>
        {
          _.map(sortedStoreIDs, storeID => {
            let replica: protos.cockroach.roachpb.ReplicaDescriptor$Properties = null;
            if (replicasByReplicaIDByStoreID.has(storeID) &&
              replicasByReplicaIDByStoreID.get(storeID).has(referenceReplica.replica_id)) {
              replica = replicasByReplicaIDByStoreID.get(storeID).get(referenceReplica.replica_id);
            }
            return this.renderRangeReplicaCell(
              leaderReplicaIDs,
              referenceReplica.replica_id,
              replica,
              rangeID,
              storeID,
              dormantStoreIDs.has(storeID),
            );
          })
        }
      </tr>
    );
  }

  render() {
    const { infos, replicas } = this.props;
    const leader = _.head(infos);
    const rangeID = leader.state.state.desc.range_id;

    // We want to display ordered by store ID.
    const sortedStoreIDs = _.chain(infos)
      .map(info => info.source_store_id)
      .sortBy(id => id)
      .value();

    const dormantStoreIDs: Set<number> = new Set();

    // Convert the infos to a simpler object for display purposes. This helps when trying to
    // determine if any warnings should be displayed.
    const detailsByStoreID: Map<number, RangeTableDetail> = new Map();
    _.forEach(infos, info => {
      const localReplica = RangeInfo.GetLocalReplica(info);
      const lease = info.state.state.lease;
      const epoch = Lease.IsEpoch(lease);
      const raftLeader = FixLong(info.raft_state.lead).eq(localReplica.replica_id);
      const leaseHolder = localReplica.replica_id === lease.replica.replica_id;
      const mvcc = info.state.state.stats;
      const raftState = this.contentRaftState(info.raft_state.state);
      if (raftState.value[0] === "dormant") {
        dormantStoreIDs.add(info.source_store_id);
      }
      detailsByStoreID.set(info.source_store_id, {
        id: this.createContent(Print.ReplicaID(rangeID, RangeInfo.GetLocalReplica(info))),
        keyRange: this.createContent(`${info.span.end_key} to ${info.span.end_key}`),
        raftState: raftState,
        leaseHolder: this.createContent(
          Print.ReplicaID(rangeID, lease.replica),
          leaseHolder ? "range-table__cell--lease-holder" : "range-table__cell--lease-follower",
        ),
        leaseType: this.createContent(epoch ? "Epoch" : "Expiration"),
        leaseEpoch: epoch ? this.createContent(lease.epoch) : rangeTableEmptyContent,
        leaseStart: this.contentTimestamp(lease.start),
        leaseExpiration: epoch ? rangeTableEmptyContent : this.contentTimestamp(lease.expiration),
        leaseAppliedIndex: this.createContent(info.state.state.lease_applied_index),
        raftLeader: this.createContent(
          info.raft_state.lead,
          raftLeader ? "range-table__cell--raftstate-leader" : "range-table__cell--raftstate-follower",
        ),
        vote: this.createContent(info.raft_state.hard_state.vote),
        term: this.createContent(info.raft_state.hard_state.term),
        applied: this.createContent(info.raft_state.applied),
        commit: this.createContent(info.raft_state.hard_state.commit),
        lastIndex: this.createContent(info.state.lastIndex),
        logSize: this.createContent(info.state.raft_log_size),
        leaseHolderQPS: leaseHolder ? this.createContent(info.stats.queries_per_second.toFixed(4)) : rangeTableEmptyContent,
        keysWrittenPS: this.createContent(info.stats.writes_per_second.toFixed(4)),
        approxProposalQuota: raftLeader ? this.createContent(info.state.approximate_proposal_quota) : rangeTableEmptyContent,
        pendingCommands: this.createContent(info.state.num_pending),
        droppedCommands: this.createContent(
          info.state.num_dropped,
          FixLong(info.state.num_dropped).greaterThan(0) ? "range-table__cell--dropped-commands" : "",
        ),
        truncatedIndex: this.createContent(info.state.state.truncated_state.index),
        truncatedTerm: this.createContent(info.state.state.truncated_state.term),
        mvccLastUpdate: this.contentNanos(mvcc.last_update_nanos),
        mvccIntentAge: this.contentDuration(mvcc.intent_age),
        mvccGGBytesAge: this.contentDuration(mvcc.gc_bytes_age),
        mvccLiveBytesCount: this.contentMVCC(mvcc.live_bytes, mvcc.live_count),
        mvccKeyBytesCount: this.contentMVCC(mvcc.key_bytes, mvcc.key_count),
        mvccValueBytesCount: this.contentMVCC(mvcc.val_bytes, mvcc.val_count),
        mvccIntentBytesCount: this.contentMVCC(mvcc.intent_bytes, mvcc.intent_count),
        mvccSystemBytesCount: this.contentMVCC(mvcc.sys_bytes, mvcc.sys_count),
      });
    });

    // To provide the replica rows a unique id, start counting after the normal rows.
    const firstKeyMax = rangeTableDisplayList.length;
    const leaderReplicaIDs = new Set(_.map(leader.state.state.desc.replicas, rep => rep.replica_id));

    // Go through all the replicas and add them to map for easy printing.
    const replicasByReplicaIDByStoreID: Map<number, Map<number, protos.cockroach.roachpb.ReplicaDescriptor$Properties>> = new Map();
    _.forEach(infos, info => {
      const replicasByReplicaID: Map<number, protos.cockroach.roachpb.ReplicaDescriptor$Properties> = new Map();
      _.forEach(info.state.state.desc.replicas, rep => {
        replicasByReplicaID.set(rep.replica_id, rep);
      });
      replicasByReplicaIDByStoreID.set(info.source_store_id, replicasByReplicaID);
    });

    return (
      <table className="range-table">
        <tbody>
          {
            _.map(rangeTableDisplayList, (title, key) => (
              this.renderRangeRow(
                title,
                detailsByStoreID,
                dormantStoreIDs,
                leader.source_store_id,
                sortedStoreIDs,
                key,
              )
            ))
          }
          {
            _.map(replicas, (replica, key) => (
              this.renderRangeReplicaRow(
                replicasByReplicaIDByStoreID,
                replica,
                leaderReplicaIDs,
                dormantStoreIDs,
                sortedStoreIDs,
                rangeID,
                firstKeyMax + key,
              )
            ))
          }
        </tbody>
      </table>
    );
  }
}
