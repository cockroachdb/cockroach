// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import Long from "long";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import {
  allocatorRangeRequestKey,
  rangeRequestKey,
  rangeLogRequestKey,
  refreshAllocatorRange,
  refreshRange,
  refreshRangeLog,
} from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
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
  range: CachedDataReducerState<protos.cockroach.server.serverpb.RangeResponse>;
  allocator: CachedDataReducerState<protos.cockroach.server.serverpb.AllocatorRangeResponse>;
  rangeLog: CachedDataReducerState<protos.cockroach.server.serverpb.RangeLogResponse>;
  refreshRange: typeof refreshRange;
  refreshAllocatorRange: typeof refreshAllocatorRange;
  refreshRangeLog: typeof refreshRangeLog;
}

type RangeProps = RangeOwnProps & RouterState;

function ErrorPage(props: {
  rangeID: string;
  errorText: string;
  range?: CachedDataReducerState<protos.cockroach.server.serverpb.RangeResponse>;
}) {
  return (
    <div className="section">
      <h1>Range Report for r{props.rangeID}</h1>
      <h2>{props.errorText}</h2>
      <ConnectionsTable range={props.range} />
    </div>
  );
}

function rangeRequestFromProps(props: RangeProps) {
  return new protos.cockroach.server.serverpb.RangeRequest({
    range_id: Long.fromString(props.params[rangeIDAttr]),
  });
}

function allocatorRequestFromProps(props: RangeProps) {
  return new protos.cockroach.server.serverpb.AllocatorRangeRequest({
    range_id: Long.fromString(props.params[rangeIDAttr]),
  });
}

function rangeLogRequestFromProps(props: RangeProps) {
  // TODO(bram): Remove this limit once #18159 is resolved.
  return new protos.cockroach.server.serverpb.RangeLogRequest({
    range_id: Long.fromString(props.params[rangeIDAttr]),
    limit: -1,
  });
}

/**
 * Renders the Range Report page.
 */
class Range extends React.Component<RangeProps, {}> {
  refresh(props = this.props) {
    props.refreshRange(rangeRequestFromProps(props));
    props.refreshAllocatorRange(allocatorRequestFromProps(props));
    props.refreshRangeLog(rangeLogRequestFromProps(props));
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
    if (!_.isNil(range) && !_.isNil(range.lastError)) {
      return (
        <ErrorPage
          rangeID={rangeID}
          errorText={`Error loading range ${range.lastError}`}
        />
      );
    }
    if (_.isNil(range) || _.isEmpty(range.data)) {
      return (
        <ErrorPage rangeID={rangeID} errorText={`Loading cluster status...`} />
      );
    }
    const responseRangeID = FixLong(range.data.range_id);
    if (!responseRangeID.eq(rangeID)) {
      return (
        <ErrorPage rangeID={rangeID} errorText={`Updating cluster status...`} />
      );
    }
    if (responseRangeID.isNegative() || responseRangeID.isZero()) {
      return (
        <ErrorPage
          rangeID={rangeID}
          errorText={`Range ID must be a positive non-zero integer. "${rangeID}"`}
        />
      );
    }

    // Did we get any responses?
    if (!_.some(range.data.responses_by_node_id, resp => resp.infos.length > 0)) {
      return (
        <ErrorPage
          rangeID={rangeID}
          errorText={`No results found, perhaps r${this.props.params[rangeIDAttr]} doesn't exist.`}
          range={range}
        />
      );
    }

    // Collect all the infos and sort them, putting the leader (or the replica
    // with the highest term, first.
    const infos = _.orderBy(
      _.flatMap(range.data.responses_by_node_id, resp => {
        if (resp.response && _.isEmpty(resp.error_message)) {
          return resp.infos;
        }
        return [];
      }),
      [
        info => RangeInfo.IsLeader(info),
        info => FixLong(info.raft_state.applied).toNumber(),
        info => FixLong(info.raft_state.hard_state.term).toNumber(),
        info => {
          const localReplica = RangeInfo.GetLocalReplica(info);
          return _.isNil(localReplica) ? 0 : localReplica.replica_id;
        },
      ],
      ["desc", "desc", "desc", "asc"],
    );

    // Gather all replica IDs.
    const replicas = _.chain(infos)
      .flatMap(info => info.state.state.desc.internal_replicas)
      .sortBy(rep => rep.replica_id)
      .sortedUniqBy(rep => rep.replica_id)
      .value();

    return (
      <div className="section">
        <Helmet>
          <title>{ `r${responseRangeID.toString()} Range | Debug` }</title>
        </Helmet>
        <h1>Range Report for r{responseRangeID.toString()}</h1>
        <RangeTable infos={infos} replicas={replicas} />
        <LeaseTable info={_.head(infos)} />
        <ConnectionsTable range={range} />
        <AllocatorOutput allocator={this.props.allocator} />
        <LogTable rangeID={responseRangeID} log={this.props.rangeLog} />
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState, props: RangeProps) {
  return {
    range: state.cachedData.range[rangeRequestKey(rangeRequestFromProps(props))],
    allocator: state.cachedData.allocatorRange[allocatorRangeRequestKey(allocatorRequestFromProps(props))],
    rangeLog: state.cachedData.rangeLog[rangeLogRequestKey(rangeLogRequestFromProps(props))],
  };
}

const actions = {
  refreshRange,
  refreshAllocatorRange,
  refreshRangeLog,
};

export default connect(mapStateToProps, actions)(Range);
