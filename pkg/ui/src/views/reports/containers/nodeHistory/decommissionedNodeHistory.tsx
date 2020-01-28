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
import { Link } from "react-router";
import moment from "moment";
import _ from "lodash";

import { AdminUIState } from "src/redux/state";
import {
  LivenessStatus,
  NodesSummary,
  nodesSummarySelector,
  partitionedStatuses,
  selectNodesSummaryValid,
} from "src/redux/nodes";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import { SortedTable } from "src/views/shared/components/sortedtable";
import { INodeStatus } from "src/util/proto";
import { LongToMoment } from "src/util/convert";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { LocalSetting } from "src/redux/localsettings";

import "./decommissionedNodeHistory.styl";

const decommissionedNodesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "nodes/decommissioned_sort_setting", (s) => s.localSettings,
);

class NodeSortedTable extends SortedTable<INodeStatus> {}

export interface DecommissionedNodeHistoryProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  nodesSummaryValid: boolean;
  status: LivenessStatus.DECOMMISSIONED;
  sortSetting: SortSetting;
  setSort: typeof decommissionedNodesSortSetting.set;
  statuses: INodeStatus[];
  nodesSummary: NodesSummary;
}

class DecommissionedNodeHistory extends React.Component<DecommissionedNodeHistoryProps> {
  componentWillMount() {
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps(props: DecommissionedNodeHistoryProps) {
    props.refreshNodes();
    props.refreshLiveness();
  }

  render() {
    const { status, statuses, nodesSummary, sortSetting, setSort } = this.props;
    if (!statuses || statuses.length === 0) {
      return null;
    }

    const statusName = _.capitalize(LivenessStatus[status]);

    return (
      <section className="section">
        <Helmet title="Decommissioned Node History | Debug" />
        <h1 className="title">Decommissioned Node History</h1>
        <div>
          <NodeSortedTable
            data={statuses}
            sortSetting={sortSetting}
            onChangeSortSetting={(setting) => setSort(setting)}
            columns={[
              {
                title: "ID",
                cell: (ns) => `n${ns.desc.node_id}`,
                sort: (ns) => ns.desc.node_id,
              },
              {
                title: "Address",
                cell: (ns) => {
                  return (
                    <div>
                      <Link to={`/node/${ns.desc.node_id}`}>{ns.desc.address.address_field}</Link>
                    </div>
                  );
                },
                sort: (ns) => ns.desc.node_id,
                className: "sort-table__cell--link",
              },
              {
                title: `${statusName} Since`,
                cell: (ns) => {
                  const liveness = nodesSummary.livenessByNodeID[ns.desc.node_id];
                  if (!liveness) {
                    return "no information";
                  }

                  const deadTime = liveness.expiration.wall_time;
                  const deadMoment = LongToMoment(deadTime);
                  return `${moment.duration(deadMoment.diff(moment())).humanize()} ago`;
                },
                sort: (ns) => {
                  const liveness = nodesSummary.livenessByNodeID[ns.desc.node_id];
                  return liveness.expiration.wall_time;
                },
              },
            ]} />
        </div>
      </section>
    );
  }
}

const mapStateToProps = (state: AdminUIState) => ({
  nodesSummaryValid: selectNodesSummaryValid(state),
  sortSetting: decommissionedNodesSortSetting.selector(state),
  status: LivenessStatus.DECOMMISSIONED,
  statuses: partitionedStatuses(state).decommissioned,
  nodesSummary: nodesSummarySelector(state),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  setSort: decommissionedNodesSortSetting.set,
};

export default connect(mapStateToProps, mapDispatchToProps)(DecommissionedNodeHistory);
