// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames";
import _ from "lodash";
import Long from "long";
import moment from "moment";
import React from "react";
import * as protos from "src/js/protos";
import { cockroach } from "src/js/protos";
import { util } from "@cockroachlabs/cluster-ui";
import { FixLong } from "src/util/fixLong";
import { Bytes } from "src/util/format";
import Lease from "src/views/reports/containers/range/lease";
import Print from "src/views/reports/containers/range/print";
import RangeInfo from "src/views/reports/containers/range/rangeInfo";

interface RangeTableProps {
  infos: protos.cockroach.server.serverpb.IRangeInfo[];
  replicas: protos.cockroach.roachpb.IReplicaDescriptor[];
}

interface RangeTableRow {
  readonly variable: string;
  readonly display: string;
  readonly compareToLeader: boolean; // When true, displays a warning when a
  // value doesn't match the leader's.
}

interface RangeTableCellContent {
  // value represents the strings to be rendered. Each one is rendered on its
  // own line (as a <li>).
  value: string[];
  // title represents the strings that will constitute the HTML title of the
  // cell (i.e. the tooltip). If nil, value is used.
  title?: string[];
  // The classes to be applied to the cell.
  className?: string[];
}

interface RangeTableDetail {
  [name: string]: RangeTableCellContent;
}

const rangeTableDisplayList: RangeTableRow[] = [
  { variable: "id", display: "ID", compareToLeader: false },
  { variable: "keyRange", display: "Key Range", compareToLeader: true },
  { variable: "problems", display: "Problems", compareToLeader: true },
  { variable: "replicaType", display: "Replica Type", compareToLeader: false },
  { variable: "raftState", display: "Raft State", compareToLeader: false },
  { variable: "quiescent", display: "Quiescent", compareToLeader: true },
  { variable: "ticking", display: "Ticking", compareToLeader: true },
  { variable: "leaseType", display: "Lease Type", compareToLeader: true },
  { variable: "leaseState", display: "Lease State", compareToLeader: true },
  { variable: "leaseHolder", display: "Lease Holder", compareToLeader: true },
  { variable: "leaseEpoch", display: "Lease Epoch", compareToLeader: true },
  {
    variable: "isLeaseholder",
    display: "Is Leaseholder",
    compareToLeader: false,
  },
  { variable: "leaseValid", display: "Lease Valid", compareToLeader: false },
  { variable: "leaseStart", display: "Lease Start", compareToLeader: true },
  {
    variable: "leaseExpiration",
    display: "Lease Expiration",
    compareToLeader: true,
  },
  {
    variable: "leaseAppliedIndex",
    display: "Lease Applied Index",
    compareToLeader: true,
  },
  { variable: "raftLeader", display: "Raft Leader", compareToLeader: true },
  { variable: "vote", display: "Vote", compareToLeader: false },
  { variable: "term", display: "Term", compareToLeader: true },
  {
    variable: "leadTransferee",
    display: "Lead Transferee",
    compareToLeader: false,
  },
  { variable: "applied", display: "Applied", compareToLeader: true },
  { variable: "commit", display: "Commit", compareToLeader: true },
  { variable: "lastIndex", display: "Last Index", compareToLeader: true },
  { variable: "logSize", display: "Log Size", compareToLeader: false },
  {
    variable: "leaseHolderQPS",
    display: "Lease Holder QPS",
    compareToLeader: false,
  },
  {
    variable: "keysWrittenPS",
    display: "Average Keys Written Per Second",
    compareToLeader: false,
  },
  {
    variable: "approxProposalQuota",
    display: "Approx Proposal Quota",
    compareToLeader: false,
  },
  {
    variable: "pendingCommands",
    display: "Pending Commands",
    compareToLeader: false,
  },
  {
    variable: "droppedCommands",
    display: "Dropped Commands",
    compareToLeader: false,
  },
  {
    variable: "truncatedIndex",
    display: "Truncated Index",
    compareToLeader: true,
  },
  {
    variable: "truncatedTerm",
    display: "Truncated Term",
    compareToLeader: true,
  },
  {
    variable: "mvccLastUpdate",
    display: "MVCC Last Update",
    compareToLeader: true,
  },
  {
    variable: "gcAvgAge",
    display: "Dead Value average age",
    compareToLeader: true,
  },
  {
    variable: "gcBytesAge",
    display: "GC Bytes Age (score)",
    compareToLeader: true,
  },
  {
    variable: "numIntents",
    display: "Intents",
    compareToLeader: true,
  },
  {
    variable: "intentAvgAge",
    display: "Intent Average Age",
    compareToLeader: true,
  },
  {
    variable: "intentAge",
    display: "Intent Age (score)",
    compareToLeader: true,
  },
  {
    variable: "mvccLiveBytesCount",
    display: "MVCC Live Bytes/Count",
    compareToLeader: true,
  },
  {
    variable: "mvccKeyBytesCount",
    display: "MVCC Key Bytes/Count",
    compareToLeader: true,
  },
  {
    variable: "mvccValueBytesCount",
    display: "MVCC Value Bytes/Count",
    compareToLeader: true,
  },
  {
    variable: "mvccIntentBytesCount",
    display: "MVCC Intent Bytes/Count",
    compareToLeader: true,
  },
  {
    variable: "mvccSystemBytesCount",
    display: "MVCC System Bytes/Count",
    compareToLeader: true,
  },
  {
    variable: "rangeMaxBytes",
    display: "Max Range Size Before Split",
    compareToLeader: true,
  },
  {
    variable: "readLatches",
    display: "Read Latches",
    compareToLeader: false,
  },
  {
    variable: "writeLatches",
    display: "Write Latches",
    compareToLeader: false,
  },
  {
    variable: "locks",
    display: "Locks",
    compareToLeader: false,
  },
  {
    variable: "locksWithWaitQueues",
    display: "Locks With Wait-Queues",
    compareToLeader: false,
  },
  {
    variable: "lockWaitQueueWaiters",
    display: "Lock Wait-Queue Waiters",
    compareToLeader: false,
  },
  {
    variable: "top_k_locks_by_wait_queue_waiters",
    display: "Top Locks By Wait-Queue Waiters",
    compareToLeader: false,
  },
  {
    variable: "closedTimestampPolicy",
    display: "Closed timestamp Policy",
    compareToLeader: true,
  },
  {
    variable: "closedTimestampRaft",
    display: "Closed timestamp - Raft",
    compareToLeader: true,
  },
  {
    variable: "closedTimestampSideTransportReplica",
    display: "Closed timestamp - side transport (replica state)",
    compareToLeader: false,
  },
  {
    variable: "closedTimestampSideTransportReplicaLAI",
    display: "Closed timestamp LAI - side transport (replica state)",
    compareToLeader: false,
  },
  {
    variable: "closedTimestampSideTransportCentral",
    display: "Closed timestamp - side transport (centralized state)",
    compareToLeader: false,
  },
  {
    variable: "closedTimestampSideTransportCentralLAI",
    display: "Closed timestamp LAI - side transport (centralized state)",
    compareToLeader: false,
  },
  {
    variable: "circuitBreakerError",
    display: "Circuit Breaker Error",
    compareToLeader: false,
  },
  {
    variable: "locality",
    display: "Locality Info",
    compareToLeader: false,
  },
];

const rangeTableEmptyContent: RangeTableCellContent = {
  value: ["-"],
};

const rangeTableEmptyContentWithWarning: RangeTableCellContent = {
  value: ["-"],
  className: ["range-table__cell--warning"],
};

const rangeTableQuiescent: RangeTableCellContent = {
  value: ["quiescent"],
  className: ["range-table__cell--quiescent"],
};

function contentReplicaType(replicaType: protos.cockroach.roachpb.ReplicaType) {
  return protos.cockroach.roachpb.ReplicaType[replicaType];
}

function convertLeaseState(
  leaseState: protos.cockroach.kv.kvserver.storagepb.LeaseState,
) {
  return protos.cockroach.kv.kvserver.storagepb.LeaseState[
    leaseState
  ].toLowerCase();
}

export default class RangeTable extends React.Component<RangeTableProps, {}> {
  cleanRaftState(state: string) {
    switch (_.toLower(state)) {
      case "statedormant":
        return "dormant";
      case "stateleader":
        return "leader";
      case "statefollower":
        return "follower";
      case "statecandidate":
        return "candidate";
      case "stateprecandidate":
        return "precandidate";
      default:
        return "unknown";
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
    const humanized = Print.Time(util.LongToMoment(nanos));
    return {
      value: [humanized],
      title: [humanized, nanos.toString()],
    };
  }

  contentDuration(nanos: Long): RangeTableCellContent {
    const humanized = Print.Duration(
      moment.duration(util.NanoToMilli(nanos.toNumber())),
    );
    return {
      value: [humanized],
      title: [humanized, nanos.toString()],
    };
  }

  contentMVCC(bytes: Long, count: Long): RangeTableCellContent {
    const humanizedBytes = Bytes(bytes.toNumber());
    return {
      value: [`${humanizedBytes} / ${count.toString()} count`],
      title: [
        `${humanizedBytes} / ${count.toString()} count`,
        `${bytes.toString()} bytes / ${count.toString()} count`,
      ],
    };
  }

  contentBytes(
    bytes: Long,
    className: string = null,
    toolTip: string = null,
  ): RangeTableCellContent {
    const humanized = Bytes(bytes.toNumber());
    if (_.isNull(className)) {
      return {
        value: [humanized],
        title: [humanized, bytes.toString()],
      };
    }
    return {
      value: [humanized],
      title: [humanized, bytes.toString(), toolTip],
      className: [className],
    };
  }

  contentGCAvgAge(
    mvcc: cockroach.storage.enginepb.IMVCCStats,
  ): RangeTableCellContent {
    if (mvcc === null) {
      return this.contentDuration(Long.fromNumber(0));
    }
    const deadBytes = mvcc.key_bytes.add(mvcc.val_bytes).sub(mvcc.live_bytes);
    if (!deadBytes.eq(0)) {
      const avgDeadByteAgeSec = mvcc.gc_bytes_age.div(deadBytes);
      return this.contentDuration(
        Long.fromNumber(util.SecondsToNano(avgDeadByteAgeSec.toNumber())),
      );
    } else {
      return this.contentDuration(Long.fromNumber(0));
    }
  }

  createContentIntentAvgAge(
    mvcc: cockroach.storage.enginepb.IMVCCStats,
  ): RangeTableCellContent {
    if (mvcc === null) {
      return this.contentDuration(Long.fromNumber(0));
    }
    if (!mvcc.intent_count.eq(0)) {
      const avgIntentAgeSec = mvcc.intent_age.div(mvcc.intent_count);
      return this.contentDuration(
        Long.fromNumber(util.SecondsToNano(avgIntentAgeSec.toNumber())),
      );
    } else {
      return this.contentDuration(Long.fromNumber(0));
    }
  }

  createContent(
    value: string | Long | number,
    className: string = null,
  ): RangeTableCellContent {
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

  contentTimestamp(
    timestamp: protos.cockroach.util.hlc.ITimestamp,
    now: moment.Moment,
  ): RangeTableCellContent {
    if (_.isNil(timestamp) || _.isNil(timestamp.wall_time)) {
      return {
        value: ["no timestamp"],
        className: ["range-table__cell--warning"],
      };
    }
    if (FixLong(timestamp.wall_time).isZero()) {
      return {
        value: [""],
        title: ["0"],
      };
    }
    const humanized = Print.Timestamp(timestamp);
    const delta = Print.TimestampDeltaFromNow(timestamp, now);
    return {
      value: [humanized, delta],
      title: [humanized, FixLong(timestamp.wall_time).toString()],
    };
  }

  contentProblems(
    problems: protos.cockroach.server.serverpb.IRangeProblems,
    awaitingGC: boolean,
  ): RangeTableCellContent {
    let results: string[] = [];
    if (problems.no_lease) {
      results = _.concat(results, "Invalid Lease");
    }
    if (problems.leader_not_lease_holder) {
      results = _.concat(results, "Leader is Not Lease holder");
    }
    if (problems.underreplicated) {
      results = _.concat(results, "Underreplicated (or slow)");
    }
    if (problems.overreplicated) {
      results = _.concat(results, "Overreplicated");
    }
    if (problems.no_raft_leader) {
      results = _.concat(results, "No Raft Leader");
    }
    if (problems.unavailable) {
      results = _.concat(results, "Unavailable");
    }
    if (problems.quiescent_equals_ticking) {
      results = _.concat(results, "Quiescent equals ticking");
    }
    if (problems.raft_log_too_large) {
      results = _.concat(results, "Raft log too large");
    }
    if (awaitingGC) {
      results = _.concat(results, "Awaiting GC");
    }
    return {
      value: results,
      title: results,
      className: results.length > 0 ? ["range-table__cell--warning"] : [],
    };
  }

  // contentIf returns an empty value if the condition is false, and if true,
  // executes and returns the content function.
  contentIf(
    showContent: boolean,
    content: () => RangeTableCellContent,
  ): RangeTableCellContent {
    if (!showContent) {
      return rangeTableEmptyContent;
    }
    return content();
  }

  renderRangeCell(
    row: RangeTableRow,
    cell: RangeTableCellContent,
    key: number,
    dormant: boolean,
    leaderCell?: RangeTableCellContent,
  ) {
    const title = _.join(_.isNil(cell.title) ? cell.value : cell.title, "\n");
    const differentFromLeader =
      !dormant &&
      !_.isNil(leaderCell) &&
      row.compareToLeader &&
      (!_.isEqual(cell.value, leaderCell.value) ||
        !_.isEqual(cell.title, leaderCell.title));
    const className = classNames(
      "range-table__cell",
      {
        "range-table__cell--dormant": dormant,
        "range-table__cell--different-from-leader-warning": differentFromLeader,
      },
      !dormant && !_.isNil(cell.className) ? cell.className : [],
    );
    return (
      <td key={key} className={className} title={title}>
        <ul className="range-entries-list">
          {_.map(cell.value, (value, k) => (
            <li key={k}>{value}</li>
          ))}
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
    const leaderDetail = detailsByStoreID.get(leaderStoreID);
    const values: Set<string> = new Set();
    if (row.compareToLeader) {
      detailsByStoreID.forEach((detail, storeID) => {
        if (!dormantStoreIDs.has(storeID)) {
          values.add(_.join(detail[row.variable].value, " "));
        }
      });
    }
    const headerClassName = classNames(
      "range-table__cell",
      "range-table__cell--header",
      { "range-table__cell--header-warning": values.size > 1 },
    );
    return (
      <tr key={key} className="range-table__row">
        <th className={headerClassName}>{row.display}</th>
        {_.map(sortedStoreIDs, storeID => {
          const cell = detailsByStoreID.get(storeID)[row.variable];
          const leaderCell =
            storeID === leaderStoreID ? null : leaderDetail[row.variable];
          return this.renderRangeCell(
            row,
            cell,
            storeID,
            dormantStoreIDs.has(storeID),
            leaderCell,
          );
        })}
      </tr>
    );
  }

  renderRangeReplicaCell(
    leaderReplicaIDs: Set<number>,
    replicaID: number,
    replica: protos.cockroach.roachpb.IReplicaDescriptor,
    rangeID: Long,
    localStoreID: number,
    dormant: boolean,
  ) {
    const differentFromLeader =
      !dormant &&
      (_.isNil(replica)
        ? leaderReplicaIDs.has(replicaID)
        : !leaderReplicaIDs.has(replica.replica_id));
    const localReplica =
      !dormant &&
      !differentFromLeader &&
      replica &&
      replica.store_id === localStoreID;
    const className = classNames({
      "range-table__cell": true,
      "range-table__cell--dormant": dormant,
      "range-table__cell--different-from-leader-warning": differentFromLeader,
      "range-table__cell--local-replica": localReplica,
    });
    if (_.isNil(replica)) {
      return (
        <td key={localStoreID} className={className}>
          -
        </td>
      );
    }
    const value = Print.ReplicaID(rangeID, replica);
    return (
      <td key={localStoreID} className={className} title={value}>
        {value}
      </td>
    );
  }

  renderRangeReplicaRow(
    replicasByReplicaIDByStoreID: Map<
      number,
      Map<number, protos.cockroach.roachpb.IReplicaDescriptor>
    >,
    referenceReplica: protos.cockroach.roachpb.IReplicaDescriptor,
    leaderReplicaIDs: Set<number>,
    dormantStoreIDs: Set<number>,
    sortedStoreIDs: number[],
    rangeID: Long,
    key: string,
  ) {
    const headerClassName = "range-table__cell range-table__cell--header";
    return (
      <tr key={key} className="range-table__row">
        <th className={headerClassName}>
          Replica {referenceReplica.replica_id} - (
          {Print.ReplicaID(rangeID, referenceReplica)})
        </th>
        {_.map(sortedStoreIDs, storeID => {
          let replica: protos.cockroach.roachpb.IReplicaDescriptor = null;
          if (
            replicasByReplicaIDByStoreID.has(storeID) &&
            replicasByReplicaIDByStoreID
              .get(storeID)
              .has(referenceReplica.replica_id)
          ) {
            replica = replicasByReplicaIDByStoreID
              .get(storeID)
              .get(referenceReplica.replica_id);
          }
          return this.renderRangeReplicaCell(
            leaderReplicaIDs,
            referenceReplica.replica_id,
            replica,
            rangeID,
            storeID,
            dormantStoreIDs.has(storeID),
          );
        })}
      </tr>
    );
  }

  render() {
    const { infos, replicas } = this.props;
    const leader = _.head(infos);
    const rangeID = leader.state.state.desc.range_id;
    const data = _.chain(infos);

    // We want to display ordered by store ID.
    const sortedStoreIDs = data
      .map(info => info.source_store_id)
      .sortBy(id => id)
      .value();

    const dormantStoreIDs: Set<number> = new Set();

    const now = moment();

    // Convert the infos to a simpler object for display purposes. This helps when trying to
    // determine if any warnings should be displayed.
    const detailsByStoreID: Map<number, RangeTableDetail> = new Map();
    _.forEach(infos, info => {
      const localReplica = RangeInfo.GetLocalReplica(info);
      const awaitingGC = _.isNil(localReplica);
      const lease = info.state.state.lease;
      const epoch = Lease.IsEpoch(lease);
      const raftLeader =
        !awaitingGC &&
        FixLong(info.raft_state.lead).eq(localReplica.replica_id);
      const leaseHolder =
        !awaitingGC && localReplica.replica_id === lease.replica.replica_id;
      const mvcc = info.state.state.stats;
      const raftState = this.contentRaftState(info.raft_state.state);
      const vote = FixLong(info.raft_state.hard_state.vote);
      let leaseState: RangeTableCellContent;
      if (_.isNil(info.lease_status)) {
        leaseState = rangeTableEmptyContentWithWarning;
      } else {
        leaseState = this.createContent(
          convertLeaseState(info.lease_status.state),
          info.lease_status.state ===
            protos.cockroach.kv.kvserver.storagepb.LeaseState.VALID
            ? ""
            : "range-table__cell--warning",
        );
      }
      const dormant = raftState.value[0] === "dormant";
      if (dormant) {
        dormantStoreIDs.add(info.source_store_id);
      }
      detailsByStoreID.set(info.source_store_id, {
        id: this.createContent(
          Print.ReplicaID(
            rangeID,
            localReplica,
            info.source_node_id,
            info.source_store_id,
          ),
        ),
        keyRange: this.createContent(
          `${info.span.start_key} to ${info.span.end_key}`,
        ),
        problems: this.contentProblems(info.problems, awaitingGC),
        replicaType: awaitingGC
          ? this.createContent("") // `problems` above will report "Awaiting GC" in this case
          : this.createContent(contentReplicaType(localReplica.type)),
        raftState: raftState,
        quiescent: info.quiescent
          ? rangeTableQuiescent
          : rangeTableEmptyContent,
        ticking: this.createContent(info.ticking.toString()),
        leaseState: leaseState,
        leaseHolder: this.createContent(
          Print.ReplicaID(rangeID, lease.replica),
          leaseHolder
            ? "range-table__cell--lease-holder"
            : "range-table__cell--lease-follower",
        ),
        leaseType: this.createContent(epoch ? "epoch" : "expiration"),
        leaseEpoch: epoch
          ? this.createContent(lease.epoch)
          : rangeTableEmptyContent,
        isLeaseholder: this.createContent(String(info.is_leaseholder)),
        leaseValid: this.createContent(String(info.lease_valid)),
        leaseStart: this.contentTimestamp(lease.start, now),
        leaseExpiration: epoch
          ? rangeTableEmptyContent
          : this.contentTimestamp(lease.expiration, now),
        leaseAppliedIndex: this.createContent(
          FixLong(info.state.state.lease_applied_index),
        ),
        raftLeader: this.contentIf(!dormant, () =>
          this.createContent(
            FixLong(info.raft_state.lead),
            raftLeader
              ? "range-table__cell--raftstate-leader"
              : "range-table__cell--raftstate-follower",
          ),
        ),
        vote: this.contentIf(!dormant, () =>
          this.createContent(vote.greaterThan(0) ? vote : "-"),
        ),
        term: this.contentIf(!dormant, () =>
          this.createContent(FixLong(info.raft_state.hard_state.term)),
        ),
        leadTransferee: this.contentIf(!dormant, () => {
          const leadTransferee = FixLong(info.raft_state.lead_transferee);
          return this.createContent(
            leadTransferee.greaterThan(0) ? leadTransferee : "-",
          );
        }),
        applied: this.contentIf(!dormant, () =>
          this.createContent(FixLong(info.raft_state.applied)),
        ),
        commit: this.contentIf(!dormant, () =>
          this.createContent(FixLong(info.raft_state.hard_state.commit)),
        ),
        lastIndex: this.createContent(FixLong(info.state.last_index)),
        logSize: this.contentBytes(
          FixLong(info.state.raft_log_size),
          info.state.raft_log_size_trusted ? "" : "range-table__cell--dormant",
          "Log size is known to not be correct. This isn't an error condition. " +
            "The log size will became exact the next time it is recomputed. " +
            "This replica does not perform log truncation (because the log might already " +
            "be truncated sufficiently).",
        ),
        leaseHolderQPS: leaseHolder
          ? this.createContent(info.stats.queries_per_second.toFixed(4))
          : rangeTableEmptyContent,
        keysWrittenPS: this.createContent(
          info.stats.writes_per_second.toFixed(4),
        ),
        approxProposalQuota: raftLeader
          ? this.createContent(FixLong(info.state.approximate_proposal_quota))
          : rangeTableEmptyContent,
        pendingCommands: this.createContent(FixLong(info.state.num_pending)),
        droppedCommands: this.createContent(
          FixLong(info.state.num_dropped),
          FixLong(info.state.num_dropped).greaterThan(0)
            ? "range-table__cell--warning"
            : "",
        ),
        truncatedIndex: this.createContent(
          FixLong(info.state.state.truncated_state.index),
        ),
        truncatedTerm: this.createContent(
          FixLong(info.state.state.truncated_state.term),
        ),
        mvccLastUpdate: this.contentNanos(FixLong(mvcc.last_update_nanos)),
        mvccLiveBytesCount: this.contentMVCC(
          FixLong(mvcc.live_bytes),
          FixLong(mvcc.live_count),
        ),
        mvccKeyBytesCount: this.contentMVCC(
          FixLong(mvcc.key_bytes),
          FixLong(mvcc.key_count),
        ),
        mvccValueBytesCount: this.contentMVCC(
          FixLong(mvcc.val_bytes),
          FixLong(mvcc.val_count),
        ),
        mvccIntentBytesCount: this.contentMVCC(
          FixLong(mvcc.intent_bytes),
          FixLong(mvcc.intent_count),
        ),
        mvccSystemBytesCount: this.contentMVCC(
          FixLong(mvcc.sys_bytes),
          FixLong(mvcc.sys_count),
        ),
        rangeMaxBytes: this.contentBytes(FixLong(info.state.range_max_bytes)),
        mvccIntentAge: this.contentDuration(FixLong(mvcc.intent_age)),

        gcAvgAge: this.contentGCAvgAge(mvcc),
        gcBytesAge: this.createContent(FixLong(mvcc.gc_bytes_age)),

        numIntents: this.createContent(FixLong(mvcc.intent_count)),
        intentAvgAge: this.createContentIntentAvgAge(mvcc),
        intentAge: this.createContent(FixLong(mvcc.intent_age)),

        readLatches: this.createContent(FixLong(info.read_latches)),
        writeLatches: this.createContent(FixLong(info.write_latches)),
        locks: this.createContent(FixLong(info.locks)),
        locksWithWaitQueues: this.createContent(
          FixLong(info.locks_with_wait_queues),
        ),
        lockWaitQueueWaiters: this.createContent(
          FixLong(info.lock_wait_queue_waiters),
        ),
        top_k_locks_by_wait_queue_waiters: this.contentIf(
          _.size(info.top_k_locks_by_wait_queue_waiters) > 0,
          () => ({
            value: _.map(
              info.top_k_locks_by_wait_queue_waiters,
              lock => `${lock.pretty_key} (${lock.waiters} waiters)`,
            ),
          }),
        ),

        closedTimestampPolicy: this.createContent(
          // We index into the enum in order to get the label string, instead of
          // the numeric value.
          cockroach.roachpb.RangeClosedTimestampPolicy[
            info.state.closed_timestamp_policy
          ],
          info.state.closed_timestamp_policy ==
            cockroach.roachpb.RangeClosedTimestampPolicy.LEAD_FOR_GLOBAL_READS
            ? "range-table__cell--global-range"
            : "",
        ),
        closedTimestampRaft: this.contentTimestamp(
          info.state.state.raft_closed_timestamp,
          now,
        ),
        closedTimestampSideTransportReplica: this.contentTimestamp(
          info.state.closed_timestamp_sidetransport_info.replica_closed,
          now,
        ),
        closedTimestampSideTransportReplicaLAI: this.createContent(
          FixLong(info.state.closed_timestamp_sidetransport_info.replica_lai),
          // Warn if the LAI hasn't applied yet.
          FixLong(info.state.state.lease_applied_index) <
            FixLong(info.state.closed_timestamp_sidetransport_info.replica_lai)
            ? "range-table__cell--warning"
            : "",
        ),
        closedTimestampSideTransportCentral: this.contentTimestamp(
          info.state.closed_timestamp_sidetransport_info.central_closed,
          now,
        ),
        closedTimestampSideTransportCentralLAI: this.createContent(
          FixLong(info.state.closed_timestamp_sidetransport_info.central_lai),
          // Warn if the LAI hasn't applied yet.
          FixLong(info.state.state.lease_applied_index) <
            FixLong(info.state.closed_timestamp_sidetransport_info.central_lai)
            ? "range-table__cell--warning"
            : "",
        ),
        circuitBreakerError: this.createContent(
          info.state.circuit_breaker_error,
        ),
        locality: this.contentIf(_.size(info.locality.tiers) > 0, () => ({
          value: _.map(
            info.locality.tiers,
            tier => `${tier.key}: ${tier.value}`,
          ),
        })),
      });
    });

    const leaderReplicaIDs = new Set(
      _.map(leader.state.state.desc.internal_replicas, rep => rep.replica_id),
    );

    // Go through all the replicas and add them to map for easy printing.
    const replicasByReplicaIDByStoreID: Map<
      number,
      Map<number, protos.cockroach.roachpb.IReplicaDescriptor>
    > = new Map();
    _.forEach(infos, info => {
      const replicasByReplicaID: Map<
        number,
        protos.cockroach.roachpb.IReplicaDescriptor
      > = new Map();
      _.forEach(info.state.state.desc.internal_replicas, rep => {
        replicasByReplicaID.set(rep.replica_id, rep);
      });
      replicasByReplicaIDByStoreID.set(
        info.source_store_id,
        replicasByReplicaID,
      );
    });

    return (
      <div>
        <h2 className="base-heading">
          Range r{rangeID.toString()} at {Print.Time(moment().utc())} UTC
        </h2>
        <table className="range-table">
          <tbody>
            {_.map(rangeTableDisplayList, (title, key) =>
              this.renderRangeRow(
                title,
                detailsByStoreID,
                dormantStoreIDs,
                leader.source_store_id,
                sortedStoreIDs,
                key,
              ),
            )}
            {_.map(replicas, (replica, key) =>
              this.renderRangeReplicaRow(
                replicasByReplicaIDByStoreID,
                replica,
                leaderReplicaIDs,
                dormantStoreIDs,
                sortedStoreIDs,
                rangeID,
                "replica" + key,
              ),
            )}
          </tbody>
        </table>
      </div>
    );
  }
}
