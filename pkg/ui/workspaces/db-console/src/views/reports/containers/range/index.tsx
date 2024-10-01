// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Button, commonStyles } from "@cockroachlabs/cluster-ui";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { ArrowLeft } from "@cockroachlabs/icons";
import flatMap from "lodash/flatMap";
import flow from "lodash/flow";
import head from "lodash/head";
import isEmpty from "lodash/isEmpty";
import isEqual from "lodash/isEqual";
import isNil from "lodash/isNil";
import orderBy from "lodash/orderBy";
import some from "lodash/some";
import sortBy from "lodash/sortBy";
import sortedUniqBy from "lodash/sortedUniqBy";
import Long from "long";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

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
import { getMatchParamByName } from "src/util/query";
import AllocatorOutput from "src/views/reports/containers/range/allocator";
import ConnectionsTable from "src/views/reports/containers/range/connectionsTable";
import LeaseTable from "src/views/reports/containers/range/leaseTable";
import LogTable from "src/views/reports/containers/range/logTable";
import RangeInfo from "src/views/reports/containers/range/rangeInfo";
import RangeTable from "src/views/reports/containers/range/rangeTable";

import IRangeInfo = cockroach.server.serverpb.IRangeInfo;

interface RangeDispatchProps {
  refreshRange: typeof refreshRange;
  refreshAllocatorRange: typeof refreshAllocatorRange;
  refreshRangeLog: typeof refreshRangeLog;
}

interface RangeStateProps {
  range: CachedDataReducerState<protos.cockroach.server.serverpb.RangeResponse>;
  allocator: CachedDataReducerState<protos.cockroach.server.serverpb.AllocatorRangeResponse>;
  rangeLog: CachedDataReducerState<protos.cockroach.server.serverpb.RangeLogResponse>;
}

type RangeOwnProps = RangeDispatchProps & RangeStateProps;

type RangeProps = RangeOwnProps & RouteComponentProps;

function ErrorPage(props: {
  rangeID: string;
  errorText: string;
  range?: CachedDataReducerState<protos.cockroach.server.serverpb.RangeResponse>;
}) {
  return (
    <div className="section">
      <h1 className="base-heading">Range Report for r{props.rangeID}</h1>
      <h2 className="base-heading">{props.errorText}</h2>
      <ConnectionsTable range={props.range} />
    </div>
  );
}

function rangeRequestFromProps(props: RouteComponentProps) {
  const rangeId = getMatchParamByName(props.match, rangeIDAttr);
  return new protos.cockroach.server.serverpb.RangeRequest({
    range_id: Long.fromString(rangeId),
  });
}

function allocatorRequestFromProps(props: RouteComponentProps) {
  const rangeId = getMatchParamByName(props.match, rangeIDAttr);
  return new protos.cockroach.server.serverpb.AllocatorRangeRequest({
    range_id: Long.fromString(rangeId),
  });
}

function rangeLogRequestFromProps(props: RouteComponentProps) {
  const rangeId = getMatchParamByName(props.match, rangeIDAttr);
  // TODO(bram): Remove this limit once #18159 is resolved.
  return new protos.cockroach.server.serverpb.RangeLogRequest({
    range_id: Long.fromString(rangeId),
    limit: -1,
  });
}

/**
 * Renders the Range Report page.
 */
export class Range extends React.Component<RangeProps, {}> {
  refresh(props = this.props) {
    props.refreshRange(rangeRequestFromProps(props));
    props.refreshAllocatorRange(allocatorRequestFromProps(props));
    props.refreshRangeLog(rangeLogRequestFromProps(props));
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentDidUpdate(prevProps: RangeProps) {
    if (!isEqual(this.props.location, prevProps.location)) {
      this.refresh(this.props);
    }
  }

  backToHotRanges = (): void => {
    this.props.history.push("/hotranges");
  };

  render() {
    const { range, match } = this.props;
    const rangeID = getMatchParamByName(match, rangeIDAttr);

    // A bunch of quick error cases.
    if (!isNil(range) && !isNil(range.lastError)) {
      return (
        <ErrorPage
          rangeID={rangeID}
          errorText={`Error loading range ${range.lastError}`}
        />
      );
    }
    if (isNil(range) || isEmpty(range.data)) {
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
    if (!some(range.data.responses_by_node_id, resp => resp.infos.length > 0)) {
      return (
        <ErrorPage
          rangeID={rangeID}
          errorText={`No results found, perhaps r${rangeID} doesn't exist.`}
          range={range}
        />
      );
    }

    // Collect all the infos and sort them, putting the leader (or the replica
    // with the highest term, first.
    const infos = orderBy(
      flatMap(range.data.responses_by_node_id, resp => {
        if (resp.response && isEmpty(resp.error_message)) {
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
          return isNil(localReplica) ? 0 : localReplica.replica_id;
        },
      ],
      ["desc", "desc", "desc", "asc"],
    );

    // Gather all replica IDs.
    const replicas = flow(
      (infos: IRangeInfo[]) =>
        flatMap(infos, info => info.state.state.desc.internal_replicas),
      descriptors => sortBy(descriptors, d => d.replica_id),
      descriptors => sortedUniqBy(descriptors, d => d.replica_id),
    )(infos);

    return (
      <div className="section">
        <Helmet title={`r${responseRangeID.toString()} Range | Debug`} />
        <Button
          onClick={this.backToHotRanges}
          type="unstyled-link"
          size="small"
          icon={<ArrowLeft fontSize={"10px"} />}
          iconPosition="left"
          className={commonStyles("small-margin")}
        >
          Hot Ranges
        </Button>
        <h1 className="base-heading">
          Range Report for r{responseRangeID.toString()}
        </h1>
        <a
          href={`/#/debug/enqueue_range?rangeID=${responseRangeID.toString()}`}
        >
          Enqueue Range
        </a>
        <RangeTable infos={infos} replicas={replicas} />
        <LeaseTable info={head(infos)} />
        <ConnectionsTable range={range} />
        <AllocatorOutput allocator={this.props.allocator} />
        <LogTable rangeID={responseRangeID} log={this.props.rangeLog} />
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState, props: RouteComponentProps) => ({
  range: state.cachedData.range[rangeRequestKey(rangeRequestFromProps(props))],
  allocator:
    state.cachedData.allocatorRange[
      allocatorRangeRequestKey(allocatorRequestFromProps(props))
    ],
  rangeLog:
    state.cachedData.rangeLog[
      rangeLogRequestKey(rangeLogRequestFromProps(props))
    ],
});

const mapDispatchToProps = {
  refreshRange,
  refreshAllocatorRange,
  refreshRangeLog,
};

export default withRouter(
  connect<
    RangeStateProps,
    RangeDispatchProps,
    RouteComponentProps,
    AdminUIState
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(Range),
);
