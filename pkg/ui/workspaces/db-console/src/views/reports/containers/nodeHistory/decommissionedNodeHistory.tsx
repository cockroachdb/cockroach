// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ColumnsConfig,
  Table,
  util,
  Timestamp,
  useNodesSummary,
} from "@cockroachlabs/cluster-ui";
import flow from "lodash/flow";
import map from "lodash/map";
import orderBy from "lodash/orderBy";
import { Moment } from "moment-timezone";
import React, { useMemo } from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { Text } from "src/components";
import { cockroach } from "src/js/protos";
import { BackToAdvanceDebug } from "src/views/reports/containers/util";

import "./decommissionedNodeHistory.scss";

import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;
import ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

interface DecommissionedNodeStatusRow {
  key: string;
  nodeId: number;
  decommissionedDate: Moment;
}

export interface DecommissionedNodeHistoryProps {
  dataSource?: DecommissionedNodeStatusRow[];
}

const sortByNodeId = (
  a: DecommissionedNodeStatusRow,
  b: DecommissionedNodeStatusRow,
) => {
  if (a.nodeId < b.nodeId) {
    return -1;
  }
  if (a.nodeId > b.nodeId) {
    return 1;
  }
  return 0;
};

const sortByDecommissioningDate = (
  a: DecommissionedNodeStatusRow,
  b: DecommissionedNodeStatusRow,
) => {
  if (a.decommissionedDate.isBefore(b.decommissionedDate)) {
    return -1;
  }
  if (a.decommissionedDate.isAfter(b.decommissionedDate)) {
    return 1;
  }
  return 0;
};

const decommissionedColumns: ColumnsConfig<DecommissionedNodeStatusRow> = [
  {
    key: "id",
    title: "ID",
    sorter: sortByNodeId,
    render: (_text: string, record: DecommissionedNodeStatusRow) => (
      <Text>{`n${record.nodeId}`}</Text>
    ),
  },
  {
    key: "decommissionedOn",
    title: "Decommissioned On",
    sorter: sortByDecommissioningDate,
    render: (_text: string, record: DecommissionedNodeStatusRow) => {
      return (
        <Timestamp
          time={record.decommissionedDate}
          format={util.DATE_FORMAT_24_TZ}
        />
      );
    },
  },
];

export function DecommissionedNodeHistory({
  history,
}: DecommissionedNodeHistoryProps & RouteComponentProps): React.ReactElement {
  const { livenessByNodeID } = useNodesSummary();

  const dataSource = useMemo(() => {
    const getDecommissionedTime = (nodeId: number) => {
      const liveness = livenessByNodeID[nodeId];
      if (!liveness) {
        return undefined;
      }
      const deadTime = liveness.expiration.wall_time;
      return util.LongToMoment(deadTime);
    };

    const decommissionedNodes = Object.values(livenessByNodeID).filter(
      liveness => {
        return liveness?.membership === MembershipStatus.DECOMMISSIONED;
      },
    );

    return flow(
      (liveness: ILiveness[]) =>
        orderBy(liveness, [l => getDecommissionedTime(l.node_id)], ["desc"]),
      (liveness: ILiveness[]) =>
        map(liveness, (l, idx: number) => ({
          key: `${idx}`,
          nodeId: l.node_id,
          decommissionedDate: getDecommissionedTime(l.node_id),
        })),
    )(decommissionedNodes);
  }, [livenessByNodeID]);

  return (
    <section className="section">
      <Helmet title="Decommissioned Node History | Debug" />
      <BackToAdvanceDebug history={history} />
      <h1 className="base-heading title">Decommissioned Node History</h1>
      <div>
        <Table
          dataSource={dataSource}
          columns={decommissionedColumns}
          noDataMessage="There are no decommissioned nodes in this cluster."
        />
      </div>
    </section>
  );
}

export default withRouter(DecommissionedNodeHistory);
