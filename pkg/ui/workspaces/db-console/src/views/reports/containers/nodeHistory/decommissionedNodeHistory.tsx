// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ColumnsConfig,
  Table,
  SortSetting,
  util,
  Timestamp,
} from "@cockroachlabs/cluster-ui";
import flow from "lodash/flow";
import map from "lodash/map";
import orderBy from "lodash/orderBy";
import { Moment } from "moment-timezone";
import * as React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import { Text } from "src/components";
import { cockroach } from "src/js/protos";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { nodesSummarySelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { BackToAdvanceDebug } from "src/views/reports/containers/util";

import "./decommissionedNodeHistory.styl";

import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;
import ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

const decommissionedNodesSortSetting = new LocalSetting<
  AdminUIState,
  SortSetting
>("nodes/decommissioned_sort_setting", s => s.localSettings);

interface DecommissionedNodeStatusRow {
  key: string;
  nodeId: number;
  decommissionedDate: Moment;
}

export interface DecommissionedNodeHistoryProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  dataSource: DecommissionedNodeStatusRow[];
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

export class DecommissionedNodeHistory extends React.Component<
  DecommissionedNodeHistoryProps & RouteComponentProps
> {
  columns: ColumnsConfig<DecommissionedNodeStatusRow> = [
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

  componentDidMount() {
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentDidUpdate() {
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  render() {
    const { dataSource } = this.props;

    return (
      <section className="section">
        <Helmet title="Decommissioned Node History | Debug" />
        <BackToAdvanceDebug history={this.props.history} />
        <h1 className="base-heading title">Decommissioned Node History</h1>
        <div>
          <Table
            dataSource={dataSource}
            columns={this.columns}
            noDataMessage="There are no decommissioned nodes in this cluster."
          />
        </div>
      </section>
    );
  }
}

const decommissionedNodesTableData = createSelector(
  nodesSummarySelector,
  (nodesSummary): DecommissionedNodeStatusRow[] => {
    const getDecommissionedTime = (nodeId: number) => {
      const liveness = nodesSummary.livenessByNodeID[nodeId];
      if (!liveness) {
        return undefined;
      }
      const deadTime = liveness.expiration.wall_time;
      return util.LongToMoment(deadTime);
    };

    const decommissionedNodes = Object.values(
      nodesSummary.livenessByNodeID,
    ).filter(liveness => {
      return liveness?.membership === MembershipStatus.DECOMMISSIONED;
    });

    const data = flow(
      (liveness: ILiveness[]) =>
        orderBy(liveness, [l => getDecommissionedTime(l.node_id)], ["desc"]),
      (liveness: ILiveness[]) =>
        map(liveness, (l, idx: number) => ({
          key: `${idx}`,
          nodeId: l.node_id,
          decommissionedDate: getDecommissionedTime(l.node_id),
        })),
    )(decommissionedNodes);

    return data;
  },
);

const mapStateToProps = (state: AdminUIState, _: RouteComponentProps) => ({
  dataSource: decommissionedNodesTableData(state),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  setSort: decommissionedNodesSortSetting.set,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(DecommissionedNodeHistory),
);
