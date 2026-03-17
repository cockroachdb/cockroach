// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Button,
  util,
  Timestamp,
  useNodesSummary,
} from "@cockroachlabs/cluster-ui";
import { ArrowLeft } from "@cockroachlabs/icons";
import map from "lodash/map";
import React from "react";
import { Helmet } from "react-helmet";
import { Link, useHistory, useRouteMatch } from "react-router-dom";

import { livenessNomenclature, LivenessStatus } from "src/redux/nodes";
import { nodeIDAttr } from "src/util/constants";
import { INodeStatus, MetricConstants, StatusMetrics } from "src/util/proto";
import { getMatchParamByName } from "src/util/query";
import {
  SummaryBar,
  SummaryLabel,
  SummaryValue,
} from "src/views/shared/components/summaryBar";

import "./nodeOverview.scss";

import {
  LiveBytesTooltip,
  KeyBytesTooltip,
  ValueBytesTooltip,
  IntentBytesTooltip,
  SystemBytesTooltip,
  NodeUsedCapacityTooltip,
  NodeAvailableCapacityTooltip,
  NodeMaximumCapacityTooltip,
  MVCCRangeKeyBytesTooltip,
  MVCCRangeValueBytesTooltip,
  CellTooltipProps,
} from "./tooltips";

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
  CellTooltip?: React.FC<CellTooltipProps>;
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
      {map(data.store_statuses, ss => {
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

/**
 * Renders the Node Overview page.
 */
export function NodeOverview(): React.ReactElement {
  const history = useHistory();
  const match = useRouteMatch();
  const nodeId = parseInt(getMatchParamByName(match, nodeIDAttr), 10);
  const { nodeStatusByID, livenessStatusByNodeID, nodeDisplayNameByID } =
    useNodesSummary();

  const node = nodeStatusByID?.[nodeId?.toString()] as INodeStatus | undefined;

  const { Bytes, Percentage, DATE_FORMAT_24_TZ } = util;

  if (!node) {
    return (
      <div className="section">
        <h1 className="base-heading">Loading cluster status...</h1>
      </div>
    );
  }

  const liveness =
    livenessStatusByNodeID[node.desc.node_id] ||
    LivenessStatus.NODE_STATUS_LIVE;
  const livenessString = livenessNomenclature(liveness);

  return (
    <div>
      <Helmet title={`${nodeDisplayNameByID[node.desc.node_id]} | Nodes`} />
      <div className="section section--heading">
        <Button
          onClick={() => history.goBack()}
          type="unstyled-link"
          size="small"
          icon={<ArrowLeft fontSize={"10px"} />}
          iconPosition="left"
        >
          Overview
        </Button>
        <h3 className="base-heading">{`Node ${node.desc.node_id} / ${node.desc.address.address_field}`}</h3>
      </div>
      <section className="section l-columns">
        <div className="l-columns__left">
          <table className="table" data-testid="node-overview-table">
            <thead>
              <tr className="table__row table__row--header">
                <th className="table__cell" />
                <th className="table__cell">{`Node ${node.desc.node_id}`}</th>
                {map(node.store_statuses, ss => {
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
                valueFn={metrics => Bytes(metrics[MetricConstants.liveBytes])}
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
                CellTooltip={LiveBytesTooltip}
              />
              <TableRow
                data={node}
                title="Key Bytes"
                valueFn={metrics => Bytes(metrics[MetricConstants.keyBytes])}
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
                CellTooltip={KeyBytesTooltip}
              />
              <TableRow
                data={node}
                title="Value Bytes"
                valueFn={metrics => Bytes(metrics[MetricConstants.valBytes])}
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
                CellTooltip={ValueBytesTooltip}
              />
              <TableRow
                data={node}
                title="MVCC Range Key Bytes"
                valueFn={metrics =>
                  Bytes(metrics[MetricConstants.rangeKeyBytes] || 0)
                }
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
                CellTooltip={MVCCRangeKeyBytesTooltip}
              />
              <TableRow
                data={node}
                title="MVCC Range Value Bytes"
                valueFn={metrics =>
                  Bytes(metrics[MetricConstants.rangeValBytes] || 0)
                }
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
                CellTooltip={MVCCRangeValueBytesTooltip}
              />
              <TableRow
                data={node}
                title="Intent Bytes"
                valueFn={metrics => Bytes(metrics[MetricConstants.intentBytes])}
                CellTooltip={IntentBytesTooltip}
              />
              <TableRow
                data={node}
                title="System Bytes"
                valueFn={metrics => Bytes(metrics[MetricConstants.sysBytes])}
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
                CellTooltip={SystemBytesTooltip}
              />
              <TableRow
                data={node}
                title="GC Bytes Age"
                valueFn={metrics =>
                  metrics[MetricConstants.gcBytesAge].toString()
                }
              />
              <TableRow
                data={node}
                title="Total Replicas"
                valueFn={metrics =>
                  metrics[MetricConstants.replicas].toString()
                }
              />
              <TableRow
                data={node}
                title="Raft Leaders"
                valueFn={metrics =>
                  metrics[MetricConstants.raftLeaders].toString()
                }
              />
              <TableRow
                data={node}
                title="Total Ranges"
                valueFn={metrics => metrics[MetricConstants.ranges]}
              />
              <TableRow
                data={node}
                title="Unavailable %"
                valueFn={metrics =>
                  Percentage(
                    metrics[MetricConstants.unavailableRanges],
                    metrics[MetricConstants.ranges],
                  )
                }
              />
              <TableRow
                data={node}
                title="Under Replicated %"
                valueFn={metrics =>
                  Percentage(
                    metrics[MetricConstants.underReplicatedRanges],
                    metrics[MetricConstants.ranges],
                  )
                }
              />
              <TableRow
                data={node}
                title="Used Capacity"
                valueFn={metrics =>
                  Bytes(metrics[MetricConstants.usedCapacity])
                }
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
                CellTooltip={NodeUsedCapacityTooltip}
              />
              <TableRow
                data={node}
                title="Available Capacity"
                valueFn={metrics =>
                  Bytes(metrics[MetricConstants.availableCapacity])
                }
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
                CellTooltip={NodeAvailableCapacityTooltip}
              />
              <TableRow
                data={node}
                title="Maximum Capacity"
                valueFn={metrics => Bytes(metrics[MetricConstants.capacity])}
                nodeName={nodeDisplayNameByID[node.desc.node_id]}
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
              value={
                <Timestamp
                  time={util.LongToMoment(node.updated_at)}
                  format={DATE_FORMAT_24_TZ}
                />
              }
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

export default NodeOverview;
