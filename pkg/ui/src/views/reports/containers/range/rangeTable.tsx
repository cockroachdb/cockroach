import classNames from "classnames";
import _ from "lodash";
import Long from "long";
import moment from "moment";
import { Link } from "react-router";
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
  readonly compareToLeader: boolean; // When true, displays a warning when a
  // value doesn't match the leader's.
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
  { variable: "problems", display: "Problems", compareToLeader: true },
  { variable: "raftState", display: "Raft State", compareToLeader: false },
  { variable: "quiescent", display: "Quiescent", compareToLeader: true },
  { variable: "leaseType", display: "Lease Type", compareToLeader: true },
  { variable: "leaseState", display: "Lease State", compareToLeader: true },
  { variable: "leaseHolder", display: "Lease Holder", compareToLeader: true },
  { variable: "leaseEpoch", display: "Lease Epoch", compareToLeader: true },
  { variable: "leaseStart", display: "Lease Start", compareToLeader: true },
  { variable: "leaseExpiration", display: "Lease Expiration", compareToLeader: true },
  { variable: "leaseAppliedIndex", display: "Lease Applied Index", compareToLeader: true },
  { variable: "raftLeader", display: "Raft Leader", compareToLeader: true },
  { variable: "vote", display: "Vote", compareToLeader: false },
  { variable: "term", display: "Term", compareToLeader: true },
  { variable: "leadTransferee", display: "Lead Transferee", compareToLeader: false },
  { variable: "applied", display: "Applied", compareToLeader: true },
  { variable: "commit", display: "Commit", compareToLeader: true },
  { variable: "lastIndex", display: "Last Index", compareToLeader: true },
  { variable: "logSize", display: "Log Size", compareToLeader: false },
  { variable: "leaseHolderQPS", display: "Lease Holder QPS", compareToLeader: false },
  { variable: "keysWrittenPS", display: "Average Keys Written Per Second", compareToLeader: false },
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
  { variable: "cmdQWrites", display: "CmdQ Writes Local/Global", compareToLeader: false },
  { variable: "cmdQReads", display: "CmdQ Reads Local/Global", compareToLeader: false },
  { variable: "cmdQMaxOverlapsSeen", display: "CmdQ Max Overlaps Local/Global", compareToLeader: false },
  { variable: "cmdQTreeSize", display: "CmdQ Tree Size Local/Global", compareToLeader: false },
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

function convertLeaseState(leaseState: protos.cockroach.storage.LeaseState) {
  return protos.cockroach.storage.LeaseState[leaseState].toLowerCase();
}

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
    return this.createContent(`${bytes.toString()} bytes / ${count.toString()} count`);
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

  contentCommandQueue(
    local: Long | number, global: Long | number, isRaftLeader: boolean,
  ): RangeTableCellContent {
    if (isRaftLeader) {
      return this.createContent(`${local.toString()} local / ${global.toString()} global`);
    }
    if (local.toString() === "0" && global.toString() === "0") {
      return rangeTableEmptyContent;
    }
    return this.createContent(
      `${local.toString()} local / ${global.toString()} global`,
      "range-table__cell--warning",
    );
  }

  contentTimestamp(timestamp: protos.cockroach.util.hlc.Timestamp$Properties): RangeTableCellContent {
    const humanized = Print.Timestamp(timestamp);
    return {
      value: [humanized],
      title: [humanized, timestamp.wall_time.toString()],
    };
  }

  contentProblems(
    problems: protos.cockroach.server.serverpb.RangeProblems$Properties,
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
    if (problems.no_raft_leader) {
      results = _.concat(results, "No Raft Leader");
    }
    if (problems.unavailable) {
      results = _.concat(results, "Unavailable");
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
    const differentFromLeader = !dormant && !_.isNil(leaderCell) && row.compareToLeader && !_.isEqual(cell.value, leaderCell.value);
    const className = classNames(
      "range-table__cell",
      {
        "range-table__cell--dormant": dormant,
        "range-table__cell--different-from-leader-warning": differentFromLeader,
      },
      (!dormant && !_.isNil(cell.className) ? cell.className : []),
    );
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
    const differentFromLeader = !dormant && (_.isNil(replica) ? leaderReplicaIDs.has(replicaID) : !leaderReplicaIDs.has(replica.replica_id));
    const localReplica = !dormant && !differentFromLeader && replica && replica.store_id === localStoreID;
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
    replicasByReplicaIDByStoreID: Map<number, Map<number, protos.cockroach.roachpb.ReplicaDescriptor$Properties>>,
    referenceReplica: protos.cockroach.roachpb.ReplicaDescriptor$Properties,
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

  renderCommandQueueVizRow(
    sortedStoreIDs: number[],
    rangeID: Long,
    leader: protos.cockroach.server.serverpb.RangeInfo$Properties,
  ) {
    const vizLink = (
      <Link
        to={`/reports/range/${rangeID}/cmdqueue`}
        className="debug-link">
        Visualize
      </Link>
    );

    return (
      <tr className="range-table__row">
        <th className="range-table__cell range-table__cell--header">
          CmdQ State
        </th>
        {sortedStoreIDs.map((storeId) => (
          <td className="range-table__cell" key={storeId}>
            {storeId === leader.source_store_id ? vizLink : "-"}
          </td>
        ))}
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
      const awaitingGC = _.isNil(localReplica);
      const lease = info.state.state.lease;
      const epoch = Lease.IsEpoch(lease);
      const raftLeader = !awaitingGC && FixLong(info.raft_state.lead).eq(localReplica.replica_id);
      const leaseHolder = !awaitingGC && localReplica.replica_id === lease.replica.replica_id;
      const mvcc = info.state.state.stats;
      const raftState = this.contentRaftState(info.raft_state.state);
      const vote = FixLong(info.raft_state.hard_state.vote);
      let leaseState: RangeTableCellContent;
      if (_.isNil(info.lease_status)) {
        leaseState = rangeTableEmptyContentWithWarning;
      } else {
        leaseState = this.createContent(
          convertLeaseState(info.lease_status.state),
          info.lease_status.state === protos.cockroach.storage.LeaseState.VALID ? "" :
            "range-table__cell--warning",
        );
      }
      const dormant = raftState.value[0] === "dormant";
      if (dormant) {
        dormantStoreIDs.add(info.source_store_id);
      }
      detailsByStoreID.set(info.source_store_id, {
        id: this.createContent(Print.ReplicaID(
          rangeID,
          localReplica,
          info.source_node_id,
          info.source_store_id,
        )),
        keyRange: this.createContent(`${info.span.start_key} to ${info.span.end_key}`),
        problems: this.contentProblems(info.problems, awaitingGC),
        raftState: raftState,
        quiescent: info.quiescent ? rangeTableQuiescent : rangeTableEmptyContent,
        leaseState: leaseState,
        leaseHolder: this.createContent(
          Print.ReplicaID(rangeID, lease.replica),
          leaseHolder ? "range-table__cell--lease-holder" : "range-table__cell--lease-follower",
        ),
        leaseType: this.createContent(epoch ? "epoch" : "expiration"),
        leaseEpoch: epoch ? this.createContent(lease.epoch) : rangeTableEmptyContent,
        leaseStart: this.contentTimestamp(lease.start),
        leaseExpiration: epoch ? rangeTableEmptyContent : this.contentTimestamp(lease.expiration),
        leaseAppliedIndex: this.createContent(FixLong(info.state.state.lease_applied_index)),
        raftLeader: this.contentIf(!dormant, () => this.createContent(
          FixLong(info.raft_state.lead),
          raftLeader ? "range-table__cell--raftstate-leader" : "range-table__cell--raftstate-follower",
        )),
        vote: this.contentIf(!dormant, () => this.createContent(vote.greaterThan(0) ? vote : "-")),
        term: this.contentIf(!dormant, () => this.createContent(FixLong(info.raft_state.hard_state.term))),
        leadTransferee: this.contentIf(!dormant, () => {
          const leadTransferee = FixLong(info.raft_state.lead_transferee);
          return this.createContent(leadTransferee.greaterThan(0) ? leadTransferee : "-");
        }),
        applied: this.contentIf(!dormant, () => this.createContent(FixLong(info.raft_state.applied))),
        commit: this.contentIf(!dormant, () => this.createContent(FixLong(info.raft_state.hard_state.commit))),
        lastIndex: this.createContent(FixLong(info.state.last_index)),
        logSize: this.createContent(FixLong(info.state.raft_log_size)),
        leaseHolderQPS: leaseHolder ? this.createContent(info.stats.queries_per_second.toFixed(4)) : rangeTableEmptyContent,
        keysWrittenPS: this.createContent(info.stats.writes_per_second.toFixed(4)),
        approxProposalQuota: raftLeader ? this.createContent(FixLong(info.state.approximate_proposal_quota)) : rangeTableEmptyContent,
        pendingCommands: this.createContent(FixLong(info.state.num_pending)),
        droppedCommands: this.createContent(
          FixLong(info.state.num_dropped),
          FixLong(info.state.num_dropped).greaterThan(0) ? "range-table__cell--warning" : "",
        ),
        truncatedIndex: this.createContent(FixLong(info.state.state.truncated_state.index)),
        truncatedTerm: this.createContent(FixLong(info.state.state.truncated_state.term)),
        mvccLastUpdate: this.contentNanos(FixLong(mvcc.last_update_nanos)),
        mvccIntentAge: this.contentDuration(FixLong(mvcc.intent_age)),
        mvccGGBytesAge: this.contentDuration(FixLong(mvcc.gc_bytes_age)),
        mvccLiveBytesCount: this.contentMVCC(FixLong(mvcc.live_bytes), FixLong(mvcc.live_count)),
        mvccKeyBytesCount: this.contentMVCC(FixLong(mvcc.key_bytes), FixLong(mvcc.key_count)),
        mvccValueBytesCount: this.contentMVCC(FixLong(mvcc.val_bytes), FixLong(mvcc.val_count)),
        mvccIntentBytesCount: this.contentMVCC(FixLong(mvcc.intent_bytes), FixLong(mvcc.intent_count)),
        mvccSystemBytesCount: this.contentMVCC(FixLong(mvcc.sys_bytes), FixLong(mvcc.sys_count)),
        cmdQWrites: this.contentCommandQueue(
          FixLong(info.cmd_q_local.write_commands),
          FixLong(info.cmd_q_global.write_commands),
          raftLeader,
        ),
        cmdQReads: this.contentCommandQueue(
          FixLong(info.cmd_q_local.read_commands),
          FixLong(info.cmd_q_global.read_commands),
          raftLeader,
        ),
        cmdQMaxOverlapsSeen: this.contentCommandQueue(
          FixLong(info.cmd_q_local.max_overlaps_seen),
          FixLong(info.cmd_q_global.max_overlaps_seen),
          raftLeader,
        ),
        cmdQTreeSize: this.contentCommandQueue(
          info.cmd_q_local.tree_size,
          info.cmd_q_global.tree_size,
          raftLeader,
        ),
      });
    });

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
      <div>
        <h2>Range r{rangeID.toString()} at {Print.Time(moment().utc())} UTC</h2>
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
            {this.renderCommandQueueVizRow(sortedStoreIDs, rangeID, leader)}
            {
              _.map(replicas, (replica, key) => (
                this.renderRangeReplicaRow(
                  replicasByReplicaIDByStoreID,
                  replica,
                  leaderReplicaIDs,
                  dormantStoreIDs,
                  sortedStoreIDs,
                  rangeID,
                  "replica" + key,
                )
              ))
            }
          </tbody>
        </table>
      </div>
    );
  }
}
