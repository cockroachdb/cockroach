import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import Long from "long";
import moment from "moment";
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import { refreshRange } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { rangeIDAttr } from "src/util/constants";
import { LongToMoment, NanoToMilli, TimestampToMoment } from "src/util/convert";

interface RangeOwnProps {
  range: protos.cockroach.server.serverpb.RangeResponse;
  lastError: Error;
  refreshRange: typeof refreshRange;
}

const dateFormat = "Y-MM-DD HH:mm:ss";

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

// fixLong deals with the fact that a Long that doesn't exist in a proto is
// returned as a constant number 0. This converts those constants back into a
// Long of value 0 or returns the original Long if it already exists.
function fixLong(value: Long | number): Long {
  if (value.toString() === "0") {
    return Long.fromInt(0);
  }
  return value as Long;
}

type RangeProps = RangeOwnProps & RouterState;

/**
 * Renders the Range Report page.
 */
class Range extends React.Component<RangeProps, {}> {
  refresh(props = this.props) {
    // TODO(bram): Check range id for errors here?
    props.refreshRange(new protos.cockroach.server.serverpb.RangeRequest({
      range_id: Long.fromString(props.params[rangeIDAttr]),
    }));
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: RangeProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  renderConnections(rangeResponse: protos.cockroach.server.serverpb.RangeResponse$Properties) {
    if (_.isNil(rangeResponse)) {
      return null;
    }
    const ids = _.chain(_.keys(rangeResponse.responses_by_node_id))
      .map(id => parseInt(id, 10))
      .sort()
      .value();
    return (
      <div>
        <h2>Connections (via Node {rangeResponse.node_id})</h2>
        <table className="connections-table">
          <tbody>
            <tr className="connections-table__row connections-table__row--header">
              <th className="connections-table__cell connections-table__cell--header">Node</th>
              <th className="connections-table__cell connections-table__cell--header">Valid</th>
              <th className="connections-table__cell connections-table__cell--header">Replicas</th>
              <th className="connections-table__cell connections-table__cell--header">Error</th>
            </tr>
            {
              _.map(ids, id => {
                const resp = rangeResponse.responses_by_node_id[id];
                let headerClassName = "connections-table__row";
                if (!resp.response || !_.isEmpty(resp.error_message)) {
                  headerClassName += " connections-table__row--warning";
                }
                return (
                  <tr key={id} className={headerClassName}>
                    <td className="connections-table__cell">n{id}</td>
                    <td className="connections-table__cell">
                      {resp.response ? "ok" : "error"}
                    </td>
                    <td className="connections-table__cell">{resp.infos.length}</td>
                    <td className="connections-table__cell">{resp.error_message}</td>
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );
  }

  // If there is no otherRangeID, it comes back as the number 0.
  renderRangeID(currentRangeID: Long, otherRangeID: Long) {
    const fixedOtherRangeID = fixLong(otherRangeID);
    const fixedCurrentRangeID = fixLong(currentRangeID);
    if (fixedOtherRangeID.eq(0)) {
      return null;
    }

    if (fixedCurrentRangeID.eq(fixedOtherRangeID)) {
      return `r${fixedOtherRangeID.toString()}`;
    }

    return (
      <a href={`#/reports/range/${fixedOtherRangeID.toString()}`}>
        r{fixedOtherRangeID.toString()}
      </a>
    );
  }

  renderLogEventType(eventType: protos.cockroach.storage.RangeLogEventType) {
    switch (eventType) {
      case protos.cockroach.storage.RangeLogEventType.add: return "Add";
      case protos.cockroach.storage.RangeLogEventType.remove: return "Remove";
      case protos.cockroach.storage.RangeLogEventType.split: return "Split";
      default: return "Unknown";
    }
  }

  renderLogInfoDescriptor(
    title: string, desc: string,
  ) {
    if (_.isEmpty(desc)) {
      return null;
    }
    return (
      <li>
        {title}: {desc}
      </li>
    );
  }

  renderLogInfo(info: protos.cockroach.server.serverpb.RangeResponse.RangeLog.PrettyInfo$Properties) {
    return (
      <ul className="log-entries-list">
        {this.renderLogInfoDescriptor("Updated Range Descriptor", info.updated_desc)}
        {this.renderLogInfoDescriptor("New Range Descriptor", info.new_desc)}
        {this.renderLogInfoDescriptor("Added Replica", info.added_replica)}
        {this.renderLogInfoDescriptor("Removed Replica", info.removed_replica)}
      </ul>
    );
  }

  renderLogTable(rangeID: Long, log: protos.cockroach.server.serverpb.RangeResponse.RangeLog$Properties) {
    if (!_.isEmpty(log.error_message)) {
      return (
        <div>
          <h2>Range Log</h2>
          There was an error retrieving the range log:
          {log.error_message}
        </div>
      );
    }

    if (_.isEmpty(log.events)) {
      return (
        <div>
          <h2>Range Log</h2>
          The range log is empty.
        </div>
      );
    }

    return (
      <div>
        <h2>Range Log</h2>
        <table className="log-table">
          <tbody>
            <tr className="log-table__row log-table__row--header">
              <th className="log-table__cell log-table__cell--header">Timestamp</th>
              <th className="log-table__cell log-table__cell--header">Store</th>
              <th className="log-table__cell log-table__cell--header">Event Type</th>
              <th className="log-table__cell log-table__cell--header">Range</th>
              <th className="log-table__cell log-table__cell--header">Other Range</th>
              <th className="log-table__cell log-table__cell--header">Info</th>
            </tr>
            {
              _.chain(log.events)
                .map((event, key) => (
                  <tr key={key} className="log-table__row">
                    <td className="log-table__cell">{TimestampToMoment(event.timestamp).format(dateFormat)}</td>
                    <td className="log-table__cell">s{event.store_id}</td>
                    <td className="log-table__cell">{this.renderLogEventType(event.event_type)}</td>
                    <td className="log-table__cell">{this.renderRangeID(rangeID, event.range_id)}</td>
                    <td className="log-table__cell">{this.renderRangeID(rangeID, event.other_range_id)}</td>
                    <td className="log-table__cell">{this.renderLogInfo(log.pretty_infos[key])}</td>
                  </tr>
                ))
                .value()
            }
          </tbody>
        </table>
      </div>
    );
  }

  renderError(
    rangeID: string,
    error: string,
    rangeResponse: protos.cockroach.server.serverpb.RangeResponse$Properties,
  ) {
    return (
      <div className="section">
        <h1>Range r{rangeID}</h1>
        <h2>{error}</h2>
        {this.renderConnections(rangeResponse)}
      </div>
    );
  }

  printReplicaID(rangeID: Long, rep: protos.cockroach.roachpb.ReplicaDescriptor$Properties) {
    return `n${rep.node_id} s${rep.store_id} r${rangeID.toString()}/${rep.replica_id}`;
  }

  getReplicaByID(
    replicaID: number, replicas: protos.cockroach.roachpb.ReplicaDescriptor$Properties[],
  ) {
    return _.find(replicas, rep => rep.replica_id === replicaID);
  }

  getLocalReplica(info: protos.cockroach.server.serverpb.RangeInfo$Properties) {
    return _.find(info.state.state.desc.replicas, rep => rep.store_id === info.source_store_id);
  }

  isLeader(info: protos.cockroach.server.serverpb.RangeInfo$Properties) {
    const localRep = this.getLocalReplica(info);
    if (_.isNil(localRep)) {
      return false;
    }
    return Long.fromInt(localRep.replica_id).eq(fixLong(info.raft_state.lead));
  }

  printTime(time: moment.Moment) {
    return time.format(dateFormat);
  }

  printTimestamp(timestamp: protos.cockroach.util.hlc.Timestamp$Properties) {
    return this.printTime(LongToMoment(timestamp.wall_time));
  }

  printDuration(duration: moment.Duration) {
    const results: string[] = [];
    if (duration.hours() > 0) {
      results.push(`${duration.hours()}h`);
    }
    if (duration.minutes() > 0) {
      results.push(`${duration.minutes()}m`);
    }
    if (duration.seconds() > 0) {
      results.push(`${duration.seconds()}s`);
    }
    const ms = _.round(duration.milliseconds());
    if (ms > 0) {
      results.push(`${ms}ms`);
    }
    if (_.isEmpty(results)) {
      return "0s";
    }
    return _.join(results, " ");
  }

  printTimestampDelta(
    newTimestamp: protos.cockroach.util.hlc.Timestamp$Properties,
    oldTimestamp: protos.cockroach.util.hlc.Timestamp$Properties,
  ) {
    if (_.isNil(oldTimestamp) || _.isNil(newTimestamp)) {
      return "";
    }
    const newTime = LongToMoment(newTimestamp.wall_time);
    const oldTime = LongToMoment(oldTimestamp.wall_time);
    const diff = moment.duration(newTime.diff(oldTime));
    return this.printDuration(diff);
  }

  isLeaseEpoch(lease: protos.cockroach.roachpb.Lease$Properties) {
    return lease.epoch.toString() !== "0";
  }

  renderLeaseCell(value: string, title: string = "") {
    if (_.isEmpty(title)) {
      return <td className="lease-table__cell" title={value}>{value}</td>;
    }
    return <td className="lease-table__cell" title={title}>{value}</td>;
  }

  renderLeaseTimestampCell(timestamp: protos.cockroach.util.hlc.Timestamp$Properties) {
    const value = this.printTimestamp(timestamp);
    return this.renderLeaseCell(value, `${value}\n${timestamp.wall_time.toString()}`);
  }

  renderLeaseTable(info: protos.cockroach.server.serverpb.RangeInfo$Properties) {
    // TODO(bram): Maybe search for the latest lease record instead of just trusting the
    // leader?
    const rangeID = info.state.state.desc.range_id;
    const header = <h2>Lease History (from {this.printReplicaID(rangeID, this.getLocalReplica(info))})</h2>;
    if (_.isEmpty(info.lease_history)) {
      return (
        <div>
          {header}
          <h3>There is no lease history for this range</h3>
        </div>
      );
    }

    const epoch = this.isLeaseEpoch(_.head(info.lease_history));
    const leaseHistory = _.reverse(info.lease_history);
    return (
      <div>
        {header}
        <table className="lease-table">
          <tbody>
            <tr className="lease-table__row lease-table__row--header">
              <th className="lease-table__cell lease-table__cell--header">Replica</th>
              {
                epoch ? <th className="lease-table__cell lease-table__cell--header">Epoch</th> : null
              }
              <th className="lease-table__cell lease-table__cell--header">Proposed</th>
              <th className="lease-table__cell lease-table__cell--header">Proposed Delta</th>
              {
                !epoch ? <th className="lease-table__cell lease-table__cell--header">Expiration</th> : null
              }
              <th className="lease-table__cell lease-table__cell--header">Start</th>
              <th className="lease-table__cell lease-table__cell--header">Start Delta</th>
            </tr>
            {
              _.chain(leaseHistory)
                .map((lease, key) => {
                  let prevProposedTimestamp: protos.cockroach.util.hlc.Timestamp$Properties = null;
                  let prevStart: protos.cockroach.util.hlc.Timestamp$Properties = null;
                  if (key < leaseHistory.length - 1) {
                    prevProposedTimestamp = leaseHistory[key + 1].proposed_ts;
                    prevStart = leaseHistory[key + 1].start;
                  }
                  return (
                    <tr key={key} className="lease-table__row">
                      {this.renderLeaseCell(this.printReplicaID(rangeID, lease.replica))}
                      {epoch ? this.renderLeaseCell(`n${lease.replica.node_id}, ${lease.epoch.toString()}`) : null}
                      {this.renderLeaseTimestampCell(lease.proposed_ts)}
                      {this.renderLeaseCell(this.printTimestampDelta(lease.proposed_ts, prevProposedTimestamp))}
                      {
                        !epoch ? this.renderLeaseTimestampCell(lease.expiration) : null
                      }
                      {this.renderLeaseTimestampCell(lease.start)}
                      {this.renderLeaseCell(this.printTimestampDelta(lease.start, prevStart))}
                    </tr>
                  );
                })
                .value()
            }
          </tbody>
        </table>
      </div>
    );
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
    const value = this.printReplicaID(rangeID, replica);
    return (
      <td key={localStoreID} className={className} title={value}>
        {value}
      </td>
    );
  }

  renderRangeReplicaRow(
    replicasByStoreIDByReplicaID: Map<number, Map<number, protos.cockroach.roachpb.ReplicaDescriptor$Properties>>,
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
          Replica {referenceReplica.replica_id} - ({this.printReplicaID(rangeID, referenceReplica)})
        </th>
        {
          _.map(sortedStoreIDs, storeID => {
            let replica: protos.cockroach.roachpb.ReplicaDescriptor$Properties = null;
            if (replicasByStoreIDByReplicaID.has(storeID) &&
              replicasByStoreIDByReplicaID.get(storeID).has(referenceReplica.replica_id)) {
              replica = replicasByStoreIDByReplicaID.get(storeID).get(referenceReplica.replica_id);
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

  contentRaftState(state: string): RangeTableCellContent {
    switch (_.toLower(state)) {
      case "statedormant": return {
        value: ["dormant"],
        className: ["range-table__cell--raftstate-dormant"],
      };
      case "stateleader": return {
        value: ["leader"],
        className: ["range-table__cell--raftstate-leader"],
      };
      case "statefollower": return {
        value: ["follower"],
        className: ["range-table__cell--raftstate-follower"],
      };
      case "statecandidate": return {
        value: ["candidate"],
        className: ["range-table__cell--raftstate-candidate"],
      };
      case "stateprecandidate": return {
        value: ["precandidate"],
        className: ["range-table__cell--raftstate-precandidate"],
      };
      default: return {
        value: ["unknown"],
        className: ["range-table__cell--raftstate-unknown"],
      };
    }
  }

  contentTimestamp(timestamp: protos.cockroach.util.hlc.Timestamp$Properties): RangeTableCellContent {
    const humanized = this.printTimestamp(timestamp);
    return {
      value: [humanized],
      title: [humanized, timestamp.wall_time.toString()],
    };
  }

  contentNanos(nanos: Long): RangeTableCellContent {
    const humanized = this.printTime(LongToMoment(nanos));
    return {
      value: [humanized],
      title: [humanized, nanos.toString()],
    };
  }

  contentDuration(nanos: Long): RangeTableCellContent {
    const humanized = this.printDuration(moment.duration(NanoToMilli(nanos.toNumber())));
    return {
      value: [humanized],
      title: [humanized, nanos.toString()],
    };
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

  contentMVCC(bytes: Long, count: Long): RangeTableCellContent {
    return this.createContent(`${bytes.toString()} bytes / ${count.toString()}`);
  }

  renderRangeTable(
    infos: protos.cockroach.server.serverpb.RangeInfo$Properties[],
    replicas: protos.cockroach.roachpb.ReplicaDescriptor$Properties[],
  ) {
    const leader = _.head(infos);
    const rangeID = leader.state.state.desc.range_id;

    // We want to display ordered by store ID.
    const sortedStoreIDs = _.chain(infos)
      .map(info => info.source_store_id)
      .sort((a, b) => a < b ? -1 : (a > b ? 1 : 0))
      .value();

    const dormantStoreIDs: Set<number> = new Set();

    // Convert the infos to a simpler object for display purposes. This helps when trying to
    // determine if any warnings should be displayed.
    const detailsByStoreID: Map<number, RangeTableDetail> = new Map();
    _.forEach(infos, info => {
      const localReplica = this.getLocalReplica(info);
      const lease = info.state.state.lease;
      const epoch = this.isLeaseEpoch(lease);
      const raftLeader = fixLong(info.raft_state.lead).eq(localReplica.replica_id);
      const leaseHolder = localReplica.replica_id === lease.replica.replica_id;
      const mvcc = info.state.state.stats;
      const raftState = this.contentRaftState(info.raft_state.state);
      if (raftState.value[0] === "dormant") {
        dormantStoreIDs.add(info.source_store_id);
      }
      detailsByStoreID.set(info.source_store_id, {
        id: this.createContent(this.printReplicaID(rangeID, this.getLocalReplica(info))),
        keyRange: this.createContent(`${info.span.end_key} to ${info.span.end_key}`),
        raftState: raftState,
        leaseHolder: this.createContent(
          this.printReplicaID(rangeID, lease.replica),
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
          fixLong(info.state.num_dropped).greaterThan(0) ? "range-table__cell--dropped-commands" : "",
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
    const leaderReplicaIDs = new Set(_.chain(leader.state.state.desc.replicas)
      .map(rep => rep.replica_id)
      // Use our own function because lodash be broken and will sort lexicographically.
      .sort((a, b) => a < b ? -1 : (a > b ? 1 : 0))
      .value());

    // Go through all the replicas and add them to map for easy printing.
    const replicasByStoreIDByReplicaID: Map<number, Map<number, protos.cockroach.roachpb.ReplicaDescriptor$Properties>> = new Map();
    _.forEach(infos, info => {
      const replicasByReplicaID: Map<number, protos.cockroach.roachpb.ReplicaDescriptor$Properties> = new Map();
      _.forEach(info.state.state.desc.replicas, rep => {
        replicasByReplicaID.set(rep.replica_id, rep);
      });
      replicasByStoreIDByReplicaID.set(info.source_store_id, replicasByReplicaID);
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
                replicasByStoreIDByReplicaID,
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

  render() {
    const rangeID = this.props.params[rangeIDAttr];
    const { range } = this.props;

    // A bunch of quick error cases.
    if (!_.isNil(this.props.lastError)) {
      return this.renderError(rangeID, `Error loading range ${this.props.lastError}`, range);
    }
    if (_.isEmpty(range)) {
      return this.renderError(rangeID, `Loading cluster status...`, range);
    }
    if (range.range_id.toString() !== rangeID) {
      return this.renderError(rangeID, `Could not parse "${rangeID}" into an integer.`, range);
    }
    if (range.range_id.toString() === "0" ||
      range.range_id.isNegative() ||
      range.range_id.isZero()) {
      // The first case is due to the fact that null longs are returned as the number 0.
      return this.renderError(rangeID, `Range ID must be greater than 0. "${rangeID}"`, range);
    }

    // Did we get any responses?
    if (!_.some(range.responses_by_node_id, resp => resp.infos.length > 0)) {
      return this.renderError(
        rangeID,
        `No results found, perhaps r${this.props.params[rangeIDAttr]} doesn't exist.`,
        range,
      );
    }

    // Collect all the infos and sort them, putting the leader (or the replica
    // with the highest term, first.
    const infos = _.orderBy(
      _.flatMap(range.responses_by_node_id, resp => {
        if (resp.response && _.isEmpty(resp.error_message)) {
          return resp.infos;
        }
        return [];
      }),
      [
        (info => this.isLeader(info)),
        (info => fixLong(info.raft_state.applied).toNumber()),
        (info => fixLong(info.raft_state.hard_state.term).toNumber()),
        (info => this.getLocalReplica(info).replica_id),
      ],
      ["desc", "desc", "desc", "asc"],
      // todo only order by store id.
    );

    // Gather all replica IDs.
    const replicas = _.chain(infos)
      .flatMap(info => info.state.state.desc.replicas)
      .map(rep => rep)
      // Use our own function because lodash be broken and will sort lexicographically.
      .sort((a, b) => a.replica_id < b.replica_id ? -1 : (a.replica_id > b.replica_id ? 1 : 0))
      .sortedUniqBy(a => a.replica_id)
      .value();

    return (
      <div>
        <h1>Range r{range.range_id.toString()} at {this.printTime(moment().utc())} UTC</h1>
        {this.renderRangeTable(infos, replicas)}
        {this.renderLeaseTable(_.head(infos))}
        {this.renderLogTable(range.range_id, range.range_log)}
        {this.renderConnections(range)}
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    range: state.cachedData.range.data,
    lastError: state.cachedData.range.lastError,
  };
}

const actions = {
  refreshRange,
};

export default connect(mapStateToProps, actions)(Range);
