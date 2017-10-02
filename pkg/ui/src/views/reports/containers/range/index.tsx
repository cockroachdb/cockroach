import _ from "lodash";
import Long from "long";
import React from "react";
import { connect } from "react-redux";
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import { refreshAllocatorRange, refreshRange, refreshRangeLog } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { rangeIDAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import ConnectionsTable from "src/views/reports/containers/range/connectionsTable";
import RangeTable from "src/views/reports/containers/range/rangeTable";
import LogTable from "src/views/reports/containers/range/logTable";
import AllocatorOutput from "src/views/reports/containers/range/allocator";
import RangeInfo from "src/views/reports/containers/range/rangeInfo";
import LeaseTable from "src/views/reports/containers/range/leaseTable";

interface RangeOwnProps {
  range: protos.cockroach.server.serverpb.RangeResponse;
  allocatorRange: protos.cockroach.server.serverpb.AllocatorRangeResponse;
  rangeLog: protos.cockroach.server.serverpb.RangeLogResponse;
  lastError: Error;
  allocatorLastError: Error;
  rangeLogLastError: Error;
  refreshRange: typeof refreshRange;
  refreshAllocatorRange: typeof refreshAllocatorRange;
  refreshRangeLog: typeof refreshRangeLog;
}

type RangeProps = RangeOwnProps & RouterState;

function ErrorPage(props: {
  rangeID: string,
  errorText: string,
  rangeResponse?: protos.cockroach.server.serverpb.RangeResponse;
}) {
  return (
    <div className="section">
      <h1>Range Report for r{props.rangeID}</h1>
      <h2>{props.errorText}</h2>
      {
        _.isNil(props.rangeResponse) ? null : <ConnectionsTable rangeResponse={props.rangeResponse} />
      }
    </div>
  );
}

/**
 * Renders the Range Report page.
 */
class Range extends React.Component<RangeProps, {}> {
  refresh(props = this.props) {
    const rangeID = Long.fromString(props.params[rangeIDAttr]);
    props.refreshRange(new protos.cockroach.server.serverpb.RangeRequest({
      range_id: rangeID,
    }));
    props.refreshAllocatorRange(new protos.cockroach.server.serverpb.AllocatorRangeRequest({
      range_id: rangeID,
    }));
    // TODO(bram): Remove this limit once #18159 is resolved.
    props.refreshRangeLog(new protos.cockroach.server.serverpb.RangeLogRequest({
      range_id: rangeID,
      limit: -1,
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

  render() {
    const rangeID = this.props.params[rangeIDAttr];
    const { range } = this.props;

    // A bunch of quick error cases.
    if (!_.isNil(this.props.lastError)) {
      return <ErrorPage
        rangeID={rangeID}
        errorText={`Error loading range ${this.props.lastError}`}
      />;
    }
    if (_.isEmpty(range)) {
      return <ErrorPage
        rangeID={rangeID}
        errorText={`Loading cluster status...`}
      />;
    }
    const responseRangeID = FixLong(range.range_id);
    if (!responseRangeID.eq(rangeID)) {
      return <ErrorPage
        rangeID={rangeID}
        errorText={`Updating cluster status...`}
      />;
    }
    if (responseRangeID.isNegative() || responseRangeID.isZero()) {
      return <ErrorPage
        rangeID={rangeID}
        errorText={`Range ID must be a positive non-zero integer. "${rangeID}"`}
      />;
    }

    // Did we get any responses?
    if (!_.some(range.responses_by_node_id, resp => resp.infos.length > 0)) {
      return <ErrorPage
        rangeID={rangeID}
        errorText={`No results found, perhaps r${this.props.params[rangeIDAttr]} doesn't exist.`}
        rangeResponse={range}
      />;
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
        (info => RangeInfo.IsLeader(info)),
        (info => FixLong(info.raft_state.applied).toNumber()),
        (info => FixLong(info.raft_state.hard_state.term).toNumber()),
        (info => {
          const localReplica = RangeInfo.GetLocalReplica(info);
          return _.isNil(localReplica) ? 0 : localReplica.replica_id;
        }),
      ],
      ["desc", "desc", "desc", "asc"],
    );

    // Gather all replica IDs.
    const replicas = _.chain(infos)
      .flatMap(info => info.state.state.desc.replicas)
      .sortBy(rep => rep.replica_id)
      .sortedUniqBy(rep => rep.replica_id)
      .value();

    return (
      <div className="section">
        <h1>Range Report for r{responseRangeID.toString()}</h1>
        <RangeTable infos={infos} replicas={replicas} />
        <LeaseTable info={_.head(infos)} />
        <ConnectionsTable rangeResponse={range} />
        <AllocatorOutput
          allocatorRangeResponse={this.props.allocatorRange}
          lastError={this.props.allocatorLastError}
        />
        <LogTable
          rangeID={responseRangeID}
          log={this.props.rangeLog}
          lastError={this.props.rangeLogLastError}
        />
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    range: state.cachedData.range.data,
    allocatorRange: state.cachedData.allocatorRange.data,
    rangeLog: state.cachedData.rangeLog.data,
    lastError: state.cachedData.range.lastError,
    allocatorLastError: state.cachedData.allocatorRange.lastError,
    rangeLogLastError: state.cachedData.rangeLog.lastError,
  };
}

const actions = {
  refreshRange,
  refreshAllocatorRange,
  refreshRangeLog,
};

export default connect(mapStateToProps, actions)(Range);
