// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Button, commonStyles, util } from "@cockroachlabs/cluster-ui";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { ArrowLeft } from "@cockroachlabs/icons";
import flatMap from "lodash/flatMap";
import flow from "lodash/flow";
import head from "lodash/head";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";
import orderBy from "lodash/orderBy";
import some from "lodash/some";
import sortBy from "lodash/sortBy";
import sortedUniqBy from "lodash/sortedUniqBy";
import Long from "long";
import React from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps, withRouter } from "react-router-dom";

import * as protos from "src/js/protos";
import { getRange, getAllocatorRange, getRangeLog } from "src/util/api";
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

function ErrorPage(props: {
  rangeID: string;
  errorText: string;
  rangeData?: protos.cockroach.server.serverpb.RangeResponse;
  rangeError?: Error;
  rangeIsLoading?: boolean;
}) {
  return (
    <div className="section">
      <h1 className="base-heading">Range Report for r{props.rangeID}</h1>
      <h2 className="base-heading">{props.errorText}</h2>
      <ConnectionsTable
        data={props.rangeData}
        error={props.rangeError}
        isLoading={props.rangeIsLoading}
      />
    </div>
  );
}

/**
 * Renders the Range Report page.
 */
export function Range({
  match,
  history,
}: RouteComponentProps): React.ReactElement {
  const rangeID = getMatchParamByName(match, rangeIDAttr);
  const rangeIdLong = Long.fromString(rangeID);

  const {
    data: rangeData,
    error: rangeError,
    isLoading: rangeIsLoading,
  } = util.useSwrWithClusterId(
    ["range", rangeID],
    () =>
      getRange(
        new protos.cockroach.server.serverpb.RangeRequest({
          range_id: rangeIdLong,
        }),
      ),
    { revalidateOnFocus: false },
  );

  const {
    data: allocatorData,
    error: allocatorError,
    isLoading: allocatorIsLoading,
  } = util.useSwrWithClusterId(
    ["allocatorRange", rangeID],
    () =>
      getAllocatorRange(
        new protos.cockroach.server.serverpb.AllocatorRangeRequest({
          range_id: rangeIdLong,
        }),
      ),
    { revalidateOnFocus: false },
  );

  const {
    data: rangeLogData,
    error: rangeLogError,
    isLoading: rangeLogIsLoading,
  } = util.useSwrWithClusterId(
    ["rangeLog", rangeID],
    () =>
      getRangeLog(
        new protos.cockroach.server.serverpb.RangeLogRequest({
          range_id: rangeIdLong,
          limit: -1,
        }),
      ),
    { revalidateOnFocus: false },
  );

  // A bunch of quick error cases.
  if (!isNil(rangeError)) {
    return (
      <ErrorPage
        rangeID={rangeID}
        errorText={`Error loading range ${rangeError}`}
      />
    );
  }
  if (rangeIsLoading || isEmpty(rangeData)) {
    return (
      <ErrorPage
        rangeID={rangeID}
        errorText={`Loading cluster status...`}
        rangeIsLoading={rangeIsLoading}
      />
    );
  }
  const responseRangeID = FixLong(rangeData.range_id);
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
  if (!some(rangeData.responses_by_node_id, resp => resp.infos.length > 0)) {
    return (
      <ErrorPage
        rangeID={rangeID}
        errorText={`No results found, perhaps r${rangeID} doesn't exist.`}
        rangeData={rangeData}
        rangeError={rangeError}
        rangeIsLoading={rangeIsLoading}
      />
    );
  }

  // Collect all the infos and sort them, putting the leader (or the replica
  // with the highest term, first.
  const infos = orderBy(
    flatMap(rangeData.responses_by_node_id, resp => {
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
        onClick={() => history.push("/hotranges")}
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
      <a href={`/#/debug/enqueue_range?rangeID=${responseRangeID.toString()}`}>
        Enqueue Range
      </a>
      <RangeTable infos={infos} replicas={replicas} />
      <LeaseTable info={head(infos)} />
      <ConnectionsTable
        data={rangeData}
        error={rangeError}
        isLoading={rangeIsLoading}
      />
      <AllocatorOutput
        data={allocatorData}
        error={allocatorError}
        isLoading={allocatorIsLoading}
      />
      <LogTable
        rangeID={responseRangeID}
        data={rangeLogData}
        error={rangeLogError}
        isLoading={rangeLogIsLoading}
      />
    </div>
  );
}

export default withRouter(Range);
