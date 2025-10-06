// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ExclamationCircleOutlined, StopOutlined } from "@ant-design/icons";
import { util } from "@cockroachlabs/cluster-ui";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Badge, Divider, Tooltip } from "antd";
import { BadgeProps } from "antd/lib/badge";
import classNames from "classnames";
import map from "lodash/map";
import React from "react";
import { Link } from "react-router-dom";

import { Empty } from "src/components/empty";
import { livenessNomenclature } from "src/redux/nodes";
import { Chip } from "src/views/app/components/chip";

import { getValueFromString, Identity, isHealthyLivenessStatus } from "..";

import "./latency.styl";

import NodeLivenessStatus = cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
import ConnectionStatus = cockroach.server.serverpb.NetworkConnectivityResponse.ConnectionStatus;

interface StdDev {
  stddev: number;
  stddevMinus2: number;
  stddevMinus1: number;
  stddevPlus1: number;
  stddevPlus2: number;
}

export interface ILatencyProps {
  displayIdentities: Identity[];
  multipleHeader: boolean;
  collapsed?: boolean;
  std?: StdDev;
  node_id?: string;
}
interface DetailedRow {
  latency: number;
  identityB: Identity;
}
type DetailedIdentity = Identity & {
  row: DetailedRow[];
  title?: string;
};

// createHeaderCell creates and decorates a header cell.
function createHeaderCell(
  identity: DetailedIdentity,
  isMultiple?: boolean,
  collapsed?: boolean,
) {
  const node = `n${identity.nodeID.toString()}`;
  const className = classNames(
    "latency-table__cell",
    "latency-table__cell--header",
    { "latency-table__cell--start": isMultiple },
  );
  const isDecommissioned =
    identity.livenessStatus === NodeLivenessStatus.NODE_STATUS_DECOMMISSIONED;

  let nodeBadgeStatus: BadgeProps["status"];

  switch (identity.livenessStatus) {
    case NodeLivenessStatus.NODE_STATUS_LIVE:
      nodeBadgeStatus = "success";
      break;
    case NodeLivenessStatus.NODE_STATUS_DRAINING:
    case NodeLivenessStatus.NODE_STATUS_UNAVAILABLE:
    case NodeLivenessStatus.NODE_STATUS_DECOMMISSIONING:
      nodeBadgeStatus = "warning";
      break;
    case NodeLivenessStatus.NODE_STATUS_DEAD:
      nodeBadgeStatus = "error";
      break;
    case NodeLivenessStatus.NODE_STATUS_DECOMMISSIONED:
    default:
      nodeBadgeStatus = "default";
  }
  // Render cell header without link to node details as it is not available
  // for dead and decommissioned nodes.
  if (!isHealthyLivenessStatus(identity.livenessStatus)) {
    return (
      <td className={className}>
        <div>
          <Tooltip
            placement="bottom"
            title={livenessNomenclature(identity.livenessStatus)}
          >
            {collapsed ? "" : node}
            {!collapsed && (
              <Badge status={nodeBadgeStatus} style={{ marginLeft: "4px" }} />
            )}
          </Tooltip>
        </div>
      </td>
    );
  }
  return (
    <td className={className}>
      <Tooltip
        placement="bottom"
        title={livenessNomenclature(identity.livenessStatus)}
      >
        <Link to={!isDecommissioned ? `/node/${identity.nodeID}` : "#"}>
          {collapsed ? "" : node}
          {!collapsed && (
            <Badge status={nodeBadgeStatus} style={{ marginLeft: "4px" }} />
          )}
        </Link>
      </Tooltip>
    </td>
  );
}

const generateCollapsedData = (
  data: [DetailedIdentity[]],
  rowLength: number,
) => {
  const collapsedData: Array<DetailedIdentity[]> = [];
  const rows: number[] = [];
  for (let x = 0; x < rowLength; x++) {
    data.forEach((dataItems: DetailedIdentity[], dataIndex: number) => {
      if (!collapsedData[dataIndex]) {
        collapsedData[dataIndex] = [{ ...dataItems[0], row: [] }];
        rows.push(data[dataIndex].length);
      }
      const maxValues: DetailedRow[] = [];
      dataItems.forEach(item => maxValues.push(item.row[x]));
      collapsedData[dataIndex][0].row.push(
        maxValues.reduce((prev, current) =>
          prev.latency === 0 || prev.latency > current.latency ? prev : current,
        ),
      );
    });
  }
  return collapsedData.map(array =>
    array.map(itemValue => {
      let rowsCount = 0;
      const newRow: DetailedRow[] = [];
      rows.forEach(row => {
        const rowEach: DetailedRow[] = [];
        for (let x = 0; x < row; x++) {
          rowEach.push(itemValue.row[rowsCount + x]);
        }
        rowsCount = rowsCount + row;
        newRow.push(
          rowEach.reduce((prev, current) =>
            prev.latency === 0 || prev.latency > current.latency
              ? prev
              : current,
          ),
        );
      });
      return { ...itemValue, row: newRow };
    }),
  );
};

const renderMultipleHeaders = (
  displayIdentities: Identity[],
  collapsed: boolean,
  nodeId: string,
  multipleHeader: boolean,
) => {
  const data: any = [];
  let rowLength = 0;
  const filteredData = displayIdentities.map(identityA => {
    const row: Array<{
      latency: number;
      identityB: Identity;
      connectivity: cockroach.server.serverpb.NetworkConnectivityResponse.ConnectionStatus;
    }> = [];
    displayIdentities.forEach(identityB => {
      const connStatus =
        identityA.connectivity?.peers[identityB.nodeID]?.status ??
        ConnectionStatus.UNKNOWN;
      const latency = util.NanoToMilli(
        identityA.connectivity?.peers[identityB.nodeID]?.latency?.nanos ?? 0,
      );
      // When source and target nodes are the same.
      if (identityA.nodeID === identityB.nodeID) {
        row.push({
          latency: 0,
          identityB,
          connectivity: undefined,
        });
      } else if (latency === 0) {
        row.push({
          latency: -1,
          identityB,
          connectivity: connStatus,
        });
      } else {
        row.push({
          latency,
          identityB,
          connectivity: connStatus,
        });
      }
    });
    rowLength = row.length;
    return { row, ...identityA };
  });
  filteredData.forEach(value => {
    const newValue = {
      ...value,
      title: multipleHeader
        ? getValueFromString(nodeId, value.locality)
        : value.locality,
    };
    if (data.length === 0) {
      data[0] = new Array(newValue);
    } else {
      if (data[data.length - 1][0].title === newValue.title) {
        data[data.length - 1].push(newValue);
        data[data.length - 1][0].rowCount = data[data.length - 1].length;
      } else {
        data[data.length] = new Array(newValue);
      }
    }
  });
  if (collapsed) {
    return generateCollapsedData(data, rowLength);
  }
  return data;
};

const getVerticalLines = (data: [DetailedIdentity[]], index: number) => {
  const values: any = [];
  let currentNumber = 0;
  data.forEach(array => {
    currentNumber = currentNumber + array.length;
    values.push(currentNumber);
  });
  return values.includes(index);
};

const getLatencyCell = (
  {
    latency,
    identityB,
    identityA,
    connectivity,
  }: {
    latency: number;
    identityB: Identity;
    identityA: DetailedIdentity;
    connectivity: cockroach.server.serverpb.NetworkConnectivityResponse.ConnectionStatus;
  },
  verticalLine: boolean,
  isMultiple?: boolean,
  std?: StdDev,
  collapsed?: boolean,
) => {
  const generateClassName = (names: string[]) =>
    classNames(
      { "latency-table__cell--end": isMultiple },
      { "latency-table__cell--start": verticalLine },
      ...names,
    );

  // Case when identityA == identityB, display empty cell.
  if (latency === 0) {
    return (
      <td
        className={generateClassName([
          "latency-table__cell",
          "latency-table__cell--self",
        ])}
      />
    );
  }

  const isHealthyTargetNode = isHealthyLivenessStatus(identityB.livenessStatus);
  const isHealthyNodes =
    isHealthyLivenessStatus(identityA.livenessStatus) && isHealthyTargetNode;

  const isEstablished = connectivity === ConnectionStatus.ESTABLISHED;
  const isErrored = isHealthyNodes && connectivity === ConnectionStatus.ERROR;
  const isEstablishing =
    isHealthyNodes && connectivity === ConnectionStatus.ESTABLISHING;
  const isUnknown = isHealthyNodes && connectivity === ConnectionStatus.UNKNOWN;

  const errorMessage = identityA.connectivity?.peers[identityB.nodeID]?.error;

  let showError = !!errorMessage;
  // Show errors for all connection statuses except ESTABLISHING status that is treated exceptionally.
  if (showError && connectivity !== ConnectionStatus.ESTABLISHING) {
    showError = true;
  } else {
    // Don't show errors for nodes that try to establish connections to dead/decommissioned nodes.
    // It will always return error as `not yet heartbeated` due to target node is not alive. Connectivity
    // endpoint fans out requests to all known nodes to check connection status and it also might initiate new
    // connections to nodes that considered not alive (it is known limitation) and in this case we don't
    // want to show this error.
    showError =
      showError &&
      isHealthyTargetNode &&
      connectivity === ConnectionStatus.ESTABLISHING;
  }

  const className = classNames({
    "latency-table__cell": true,
    "latency-table__cell--end": isMultiple,
    "latency-table__cell--start": verticalLine,
    "latency-table__cell--stddev-minus-2":
      std.stddev > 0 && latency < std.stddevMinus2,
    "latency-table__cell--stddev-minus-1":
      std.stddev > 0 &&
      latency < std.stddevMinus1 &&
      latency >= std.stddevMinus2,
    "latency-table__cell--stddev-even":
      std.stddev > 0 &&
      latency >= std.stddevMinus1 &&
      latency <= std.stddevPlus1,
    "latency-table__cell--stddev-plus-1":
      std.stddev > 0 && latency > std.stddevPlus1 && latency <= std.stddevPlus2,
    "latency-table__cell--stddev-plus-2":
      std.stddev > 0 && latency > std.stddevPlus2,
  });
  const type: any = classNames({
    _: !isHealthyTargetNode,
    red: isErrored,
    yellow: isEstablishing || isUnknown,
    green: latency > 0 && std.stddev > 0 && latency < std.stddevMinus2,
    lightgreen:
      latency > 0 &&
      std.stddev > 0 &&
      latency < std.stddevMinus1 &&
      latency >= std.stddevMinus2,
    grey:
      latency > 0 &&
      std.stddev > 0 &&
      latency >= std.stddevMinus1 &&
      latency <= std.stddevPlus1,
    lightblue:
      latency > 0 &&
      std.stddev > 0 &&
      latency > std.stddevPlus1 &&
      latency <= std.stddevPlus2,
    blue: latency > 0 && std.stddev > 0 && latency > std.stddevPlus2,
  });
  const renderDescription = (data: string) => {
    if (!data) {
      return;
    }
    return map(data.split(","), (identity, index) => (
      <p key={index} className="Chip--tooltip__nodes--item-description">
        {`${identity},`}
      </p>
    ));
  };
  return (
    <td className={className}>
      {collapsed ? (
        <Chip title={`${latency.toFixed(2)}ms`} type={type} />
      ) : (
        <Tooltip
          overlayClassName="Chip--tooltip"
          placement="bottom"
          title={
            <div>
              <div className="Chip--tooltip__nodes">
                <div className="Chip--tooltip__nodes--item">
                  <div className="Chip--tooltip__nodes--item-title Chip--tooltip__nodes--header">
                    From
                  </div>
                  <p className="Chip--tooltip__nodes--item-title">{`Node ${identityA.nodeID}`}</p>
                  {renderDescription(identityA.locality)}
                </div>
                <Divider type="vertical" />
                <div className="Chip--tooltip__nodes--item">
                  <div className="Chip--tooltip__nodes--item-title Chip--tooltip__nodes--header">
                    To
                  </div>
                  <p className="Chip--tooltip__nodes--item-title">{`Node ${identityB.nodeID}`}</p>
                  {renderDescription(identityB.locality)}
                </div>
              </div>
              {latency > 0 && isEstablished && (
                <p
                  className={`color--${type} Chip--tooltip__latency`}
                >{`${latency.toFixed(2)}ms roundtrip`}</p>
              )}
              {isErrored && (
                <p className={`color--${type} Chip--tooltip__latency`}>
                  Failed connection
                </p>
              )}
              {isEstablishing && (
                <p className={`color--${type} Chip--tooltip__latency`}>
                  Attempting to connect
                </p>
              )}
              {isUnknown && (
                <p className={`color--${type} Chip--tooltip__latency`}>
                  Unknown connection state
                </p>
              )}
              {/* Show errors for ESTABLISHING status only if target node is */}
              {showError && (
                <p className={`color--red Chip--tooltip__latency`}>
                  {errorMessage}
                </p>
              )}
            </div>
          }
        >
          <div>
            <Chip
              title={
                isErrored ? (
                  <StopOutlined />
                ) : isEstablishing ? (
                  "--"
                ) : isUnknown ? (
                  <ExclamationCircleOutlined />
                ) : latency > 0 ? (
                  latency.toFixed(2) + "ms"
                ) : (
                  "--"
                )
              }
              type={type}
            />
          </div>
        </Tooltip>
      )}
    </td>
  );
};

export const Latency: React.SFC<ILatencyProps> = ({
  displayIdentities,
  multipleHeader,
  collapsed,
  std,
  node_id,
}) => {
  const data = renderMultipleHeaders(
    displayIdentities,
    collapsed,
    node_id,
    multipleHeader,
  );
  const className = classNames(
    "latency-table",
    { "latency-table__multiple": multipleHeader },
    { "latency-table__empty": data.length === 0 },
  );
  const width =
    data &&
    data.reduce((a: any, b: any) => (a.length || a) + b.length, 0) * 108 + 150;
  if (data.length === 0) {
    return (
      <div className={className}>
        <Empty />
      </div>
    );
  }
  return (
    <table className={className} style={{ width }}>
      <thead>
        {multipleHeader && (
          <tr>
            <th style={{ width: 115 }} />
            <th style={{ width: 45 }} />
            {map(data, (value, index) => (
              <th
                className="region-name"
                colSpan={data[index].length}
                key={index}
              >
                {value[0].title}
              </th>
            ))}
          </tr>
        )}
        {!collapsed && (
          <tr className="latency-table__row">
            {multipleHeader && <td />}
            <td className="latency-table__cell latency-table__cell--spacer" />
            {React.Children.toArray(
              map(data, value =>
                map(value, (identity, index: number) =>
                  createHeaderCell(identity, index === 0, collapsed),
                ),
              ),
            )}
          </tr>
        )}
      </thead>
      <tbody>
        {React.Children.toArray(
          map(data, (value, index) =>
            map(data[index], (identityA, indA: number) => {
              return (
                <tr
                  className={`latency-table__row ${
                    data[index].length === indA + 1
                      ? "latency-table__row--end"
                      : ""
                  }`}
                  key={`${index}-${indA}`}
                >
                  {multipleHeader && Number(indA) === 0 && (
                    <th rowSpan={collapsed ? 1 : data[index][0].rowCount}>
                      {value[0].title}
                    </th>
                  )}
                  {createHeaderCell(identityA, false, collapsed)}
                  {map(identityA.row, (identity: any, indexB: number) =>
                    getLatencyCell(
                      { ...identity, identityA },
                      getVerticalLines(data, indexB),
                      false,
                      std,
                      collapsed,
                    ),
                  )}
                </tr>
              );
            }),
          ),
        )}
      </tbody>
    </table>
  );
};
