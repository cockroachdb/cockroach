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
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import {
  livenessNomenclature,
  LivenessStatus,
  NodesSummary,
  nodesSummarySelector,
  selectNodesSummaryValid,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { LongToMoment } from "src/util/convert";
import { Bytes, DATE_FORMAT, Percentage } from "src/util/format";
import { INodeStatus, MetricConstants, StatusMetrics } from "src/util/proto";
import { getMatchParamByName } from "src/util/query";
import {
  SummaryBar,
  SummaryLabel,
  SummaryValue,
} from "src/views/shared/components/summaryBar";
import { Button } from "@cockroachlabs/cluster-ui";
import { ArrowLeft } from "@cockroachlabs/icons";
import "./nodeOverview.styl";
import {
  LiveBytesTooltip,
  KeyBytesTooltip,
  ValueBytesTooltip,
  IntentBytesTooltip,
  SystemBytesTooltip,
  NodeUsedCapacityTooltip,
  NodeAvailableCapacityTooltip,
  NodeMaximumCapacityTooltip,
} from "./tooltips";
import { TooltipProps } from "src/components/tooltip/tooltip";

/**
 * TableRow is a small stateless component that renders a single row in the node
 * overview table. Each row renders a store metrics value, comparing the value
 * across the different stores on the node (along with a total value for the
 * node itself).
 */
function TableRow(props: {
  data: INodeStatus;
  title: string;
  valueFn: (s: StatusMetrics) => React.ReactNode;
  CellTooltip?: React.FC<TooltipProps>;
  nodeName?: string;
}) {
  const { data, title, valueFn, CellTooltip } = props;
  return (
    <tr className="table__row table__row--body">
      <td className="table__cell">
        {CellTooltip !== undefined ? (
          <CellTooltip {...props}>{title}</CellTooltip>
        ) : (
          title
        )}
      </td>
      <td className="table__cell">{valueFn(data.metrics)}</td>
      {_.map(data.store_statuses, (ss) => {
        return (
          <td key={ss.desc.store_id} className="table__cell">
            {valueFn(ss.metrics)}
          </td>
        );
      })}
      <td className="table__cell table__cell--filler" />
    </tr>
  );
}

interface NodeOverviewProps extends RouteComponentProps {
  node: INodeStatus;
  nodesSummary: NodesSummary;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  // True if current status results are still valid. Needed so that this
  // component refreshes status query when it becomes invalid.
  nodesSummaryValid: boolean;
}

/**
 * Renders the Node Overview page.
 */
export class NodeOverview extends React.Component<NodeOverviewProps, {}> {
  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentDidUpdate() {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  prevPage = () => this.props.history.goBack();

  render() {
    const { node, nodesSummary } = this.props;
    if (!node) {
      return (
        <div className="section">
          <h1 className="base-heading">Loading cluster status...</h1>
        </div>
      );
    }

    const liveness =
      nodesSummary.livenessStatusByNodeID[node.desc.node_id] ||
      LivenessStatus.NODE_STATUS_LIVE;
    const livenessString = livenessNomenclature(liveness);

    return (
      <div>
        <Helmet
          title={`${
            nodesSummary.nodeDisplayNameByID[node.desc.node_id]
          } | Nodes`}
        />
        <div className="section section--heading">
          <Button
            onClick={this.prevPage}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
            iconPosition="left"
          >
            Overview
          </Button>
          <h2 className="base-heading">{`Node ${node.desc.node_id} / ${node.desc.address.address_field}`}</h2>
        </div>
        <section className="section l-columns">
          <div className="l-columns__left">
            <table className="table">
              <thead>
                <tr className="table__row table__row--header">
                  <th className="table__cell" />
                  <th className="table__cell">{`Node ${node.desc.node_id}`}</th>
                  {_.map(node.store_statuses, (ss) => {
                    const storeId = ss.desc.store_id;
                    return (
                      <th
                        key={storeId}
                        className="table__cell"
                      >{`Store ${storeId}`}</th>
                    );
                  })}
                  <th className="table__cell table__cell--filler" />
                </tr>
              </thead>
              <tbody>
                <TableRow
                  data={node}
                  title="Live Bytes"
                  valueFn={(metrics) =>
                    Bytes(metrics[MetricConstants.liveBytes])
                  }
                  nodeName={nodesSummary.nodeDisplayNameByID[node.desc.node_id]}
                  CellTooltip={LiveBytesTooltip}
                />
                <TableRow
                  data={node}
                  title="Key Bytes"
                  valueFn={(metrics) =>
                    Bytes(metrics[MetricConstants.keyBytes])
                  }
                  nodeName={nodesSummary.nodeDisplayNameByID[node.desc.node_id]}
                  CellTooltip={KeyBytesTooltip}
                />
                <TableRow
                  data={node}
                  title="Value Bytes"
                  valueFn={(metrics) =>
                    Bytes(metrics[MetricConstants.valBytes])
                  }
                  nodeName={nodesSummary.nodeDisplayNameByID[node.desc.node_id]}
                  CellTooltip={ValueBytesTooltip}
                />
                <TableRow
                  data={node}
                  title="Intent Bytes"
                  valueFn={(metrics) =>
                    Bytes(metrics[MetricConstants.intentBytes])
                  }
                  CellTooltip={IntentBytesTooltip}
                />
                <TableRow
                  data={node}
                  title="System Bytes"
                  valueFn={(metrics) =>
                    Bytes(metrics[MetricConstants.sysBytes])
                  }
                  nodeName={nodesSummary.nodeDisplayNameByID[node.desc.node_id]}
                  CellTooltip={SystemBytesTooltip}
                />
                <TableRow
                  data={node}
                  title="GC Bytes Age"
                  valueFn={(metrics) =>
                    metrics[MetricConstants.gcBytesAge].toString()
                  }
                />
                <TableRow
                  data={node}
                  title="Total Replicas"
                  valueFn={(metrics) =>
                    metrics[MetricConstants.replicas].toString()
                  }
                />
                <TableRow
                  data={node}
                  title="Raft Leaders"
                  valueFn={(metrics) =>
                    metrics[MetricConstants.raftLeaders].toString()
                  }
                />
                <TableRow
                  data={node}
                  title="Total Ranges"
                  valueFn={(metrics) => metrics[MetricConstants.ranges]}
                />
                <TableRow
                  data={node}
                  title="Unavailable %"
                  valueFn={(metrics) =>
                    Percentage(
                      metrics[MetricConstants.unavailableRanges],
                      metrics[MetricConstants.ranges],
                    )
                  }
                />
                <TableRow
                  data={node}
                  title="Under Replicated %"
                  valueFn={(metrics) =>
                    Percentage(
                      metrics[MetricConstants.underReplicatedRanges],
                      metrics[MetricConstants.ranges],
                    )
                  }
                />
                <TableRow
                  data={node}
                  title="Used Capacity"
                  valueFn={(metrics) =>
                    Bytes(metrics[MetricConstants.usedCapacity])
                  }
                  nodeName={nodesSummary.nodeDisplayNameByID[node.desc.node_id]}
                  CellTooltip={NodeUsedCapacityTooltip}
                />
                <TableRow
                  data={node}
                  title="Available Capacity"
                  valueFn={(metrics) =>
                    Bytes(metrics[MetricConstants.availableCapacity])
                  }
                  nodeName={nodesSummary.nodeDisplayNameByID[node.desc.node_id]}
                  CellTooltip={NodeAvailableCapacityTooltip}
                />
                <TableRow
                  data={node}
                  title="Maximum Capacity"
                  valueFn={(metrics) =>
                    Bytes(metrics[MetricConstants.capacity])
                  }
                  nodeName={nodesSummary.nodeDisplayNameByID[node.desc.node_id]}
                  CellTooltip={NodeMaximumCapacityTooltip}
                />
              </tbody>
            </table>
          </div>
          <div className="l-columns__right">
            <SummaryBar>
              <SummaryLabel>Node Summary</SummaryLabel>
              <SummaryValue
                title="Health"
                value={livenessString}
                classModifier={livenessString}
              />
              <SummaryValue
                title="Last Update"
                value={LongToMoment(node.updated_at).format(DATE_FORMAT)}
              />
              <SummaryValue title="Build" value={node.build_info.tag} />
              <SummaryValue
                title="Logs"
                value={
                  <Link to={`/node/${node.desc.node_id}/logs`}>View Logs</Link>
                }
                classModifier="link"
              />
            </SummaryBar>
          </div>
        </section>
      </div>
    );
  }
}

export const currentNode = createSelector(
  (state: AdminUIState, _props: RouteComponentProps): INodeStatus[] =>
    state.cachedData.nodes.data,
  (_state: AdminUIState, props: RouteComponentProps): number =>
    parseInt(getMatchParamByName(props.match, nodeIDAttr), 10),
  (nodes, id) => {
    if (!nodes || !id) {
      return undefined;
    }
    return _.find(nodes, (ns) => ns.desc.node_id === id);
  },
);

export default withRouter(
  connect(
    (state: AdminUIState, ownProps: RouteComponentProps) => {
      return {
        node: currentNode(state, ownProps),
        nodesSummary: nodesSummarySelector(state),
        nodesSummaryValid: selectNodesSummaryValid(state),
      };
    },
    {
      refreshNodes,
      refreshLiveness,
    },
  )(NodeOverview),
);
