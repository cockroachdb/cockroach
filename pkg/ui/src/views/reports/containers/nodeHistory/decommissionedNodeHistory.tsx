// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, withRouter } from "react-router-dom";
import { Moment } from "moment";
import _ from "lodash";

import { AdminUIState } from "src/redux/state";
import {
  nodesSummarySelector,
  partitionedStatuses,
} from "src/redux/nodes";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import { INodeStatus } from "src/util/proto";
import { LongToMoment } from "src/util/convert";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { LocalSetting } from "src/redux/localsettings";

import "./decommissionedNodeHistory.styl";
import { ColumnsConfig, Table, Text } from "src/components";
import { createSelector } from "reselect";

const decommissionedNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/decommissioned_sort_setting", (s) => s.localSettings,
);

interface DecommissionedNodeStatusRow {
  key: string;
  nodeId: number;
  address: string;
  decommissionedDate: Moment;
}

export interface DecommissionedNodeHistoryProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  dataSource: DecommissionedNodeStatusRow[];
}

export class DecommissionedNodeHistory extends React.Component<DecommissionedNodeHistoryProps> {
  columns: ColumnsConfig<DecommissionedNodeStatusRow> = [
    {
      key: "id",
      title: "ID",
      sorter: true,
      render: (_text, record) => (
        <Text>{`n${record.nodeId}`}</Text>
      ),
    },
    {
      key: "address",
      title: "Address",
      sorter: true,
      render: (_text, record) => (
        <Link to={`/node/${record.nodeId}`}>
          <Text>{record.address}</Text>
        </Link>),
    },
    {
      key: "decommissionedOn",
      title: "Decommissioned On",
      sorter: true,
      render: (_text, record) => {
        return record.decommissionedDate.format("LL[ at ]h:mm a");
      },
    },
  ];

  componentWillMount() {
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps(props: DecommissionedNodeHistoryProps) {
    props.refreshNodes();
    props.refreshLiveness();
  }

  render() {
    const { dataSource } = this.props;

    return (
      <section className="section">
        <Helmet title="Decommissioned Node History | Debug" />
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
  partitionedStatuses,
  nodesSummarySelector,
  (statuses, nodesSummary): DecommissionedNodeStatusRow[] => {
    const decommissionedStatuses = statuses.decommissioned || [];

    const getDecommissionedTime = (nodeId: number) => {
      const liveness = nodesSummary.livenessByNodeID[nodeId];
      if (!liveness) {
        return undefined;
      }
      const deadTime = liveness.expiration.wall_time;
      return LongToMoment(deadTime);
    };

    // DecommissionedNodeList displays 5 most recent nodes.
    const data = _.chain(decommissionedStatuses)
      .orderBy([(ns: INodeStatus) => getDecommissionedTime(ns.desc.node_id)], ["desc"])
      .map((ns: INodeStatus, idx: number) => {
        return {
          key: `${idx}`,
          nodeId: ns.desc.node_id,
          address: ns.desc.address.address_field,
          decommissionedDate: getDecommissionedTime(ns.desc.node_id),
        };
      })
      .value();
    return data;
  });

const mapStateToProps = (state: AdminUIState) => ({
  dataSource: decommissionedNodesTableData(state),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  setSort: decommissionedNodesSortSetting.set,
};

export default withRouter(connect(mapStateToProps, mapDispatchToProps)(DecommissionedNodeHistory));
