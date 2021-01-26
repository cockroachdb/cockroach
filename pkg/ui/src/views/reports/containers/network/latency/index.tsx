// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Divider, Tooltip } from "antd";
import classNames from "classnames";
import _ from "lodash";
import { NanoToMilli } from "src/util/convert";
import { FixLong } from "src/util/fixLong";
import { Chip } from "src/views/app/components/chip";
import React from "react";
import { Link } from "react-router-dom";
import { getValueFromString, Identity } from "..";
import "./latency.styl";
import { Empty } from "src/components/empty";

interface StdDev {
  stddev: number;
  stddevMinus2: number;
  stddevMinus1: number;
  stddevPlus1: number;
  stddevPlus2: number;
}

export interface ILatencyProps {
  displayIdentities: Identity[];
  staleIDs: Set<number>;
  multipleHeader: boolean;
  collapsed?: boolean;
  nodesSummary: any;
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
  staleIDs: Set<number>,
  id: DetailedIdentity,
  key: string,
  isMultiple?: boolean,
  collapsed?: boolean,
) {
  const node = `n${id.nodeID.toString()}`;
  const className = classNames(
    "latency-table__cell",
    "latency-table__cell--header",
    { "latency-table__cell--header-warning": staleIDs.has(id.nodeID) },
    { "latency-table__cell--start": isMultiple },
  );
  return (
    <td key={key} className={className}>
      <Link to={`/node/${id.nodeID}`}>{collapsed ? "" : node}</Link>
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
      dataItems.forEach((item) => maxValues.push(item.row[x]));
      collapsedData[dataIndex][0].row.push(
        maxValues.reduce((prev, current) =>
          prev.latency === 0 || prev.latency > current.latency ? prev : current,
        ),
      );
    });
  }
  return collapsedData.map((array) =>
    array.map((itemValue) => {
      let rowsCount = 0;
      const newRow: DetailedRow[] = [];
      rows.forEach((row) => {
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
  nodesSummary: any,
  staleIDs: Set<number>,
  nodeId: string,
  multipleHeader: boolean,
) => {
  const data: any = [];
  let rowLength = 0;
  const filteredData = displayIdentities.map((identityA) => {
    const row: any[] = [];
    displayIdentities.forEach((identityB) => {
      const a = nodesSummary.nodeStatusByID[identityA.nodeID].activity;
      const nano = FixLong(a[identityB.nodeID].latency);
      if (identityA.nodeID === identityB.nodeID) {
        row.push({ latency: 0, identityB });
      } else if (
        staleIDs.has(identityA.nodeID) ||
        staleIDs.has(identityB.nodeID) ||
        _.isNil(a) ||
        _.isNil(a[identityB.nodeID])
      ) {
        row.push({ latency: -2, identityB });
      } else if (nano.eq(0)) {
        row.push({ latency: -1, identityB });
      } else {
        const latency = NanoToMilli(nano.toNumber());
        row.push({ latency, identityB });
      }
    });
    rowLength = row.length;
    return { row, ...identityA };
  });
  filteredData.forEach((value) => {
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
  data.forEach((array) => {
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
  }: { latency: number; identityB: Identity; identityA: DetailedIdentity },
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
  if (latency === -1) {
    return (
      <td
        className={generateClassName([
          "latency-table__cell",
          "latency-table__cell--stddev-even",
        ])}
      >
        <Chip title="loading..." type="yellow" />
      </td>
    );
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
    yellow: latency === -2,
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
    return _.map(data.split(","), (identity, index) => (
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
                  <p className="Chip--tooltip__nodes--item-title">{`Node ${identityB.nodeID}`}</p>
                  {renderDescription(identityB.locality)}
                </div>
                <Divider type="vertical" />
                <div className="Chip--tooltip__nodes--item">
                  <p className="Chip--tooltip__nodes--item-title">{`Node ${identityA.nodeID}`}</p>
                  {renderDescription(identityA.locality)}
                </div>
              </div>
              {latency > 0 && (
                <p
                  className={`color--${type} Chip--tooltip__latency`}
                >{`${latency.toFixed(2)}ms roundtrip`}</p>
              )}
            </div>
          }
        >
          <div>
            <Chip
              title={latency > 0 ? latency.toFixed(2) + "ms" : "--"}
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
  staleIDs,
  multipleHeader,
  collapsed,
  nodesSummary,
  std,
  node_id,
}) => {
  const data = renderMultipleHeaders(
    displayIdentities,
    collapsed,
    nodesSummary,
    staleIDs,
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
            {_.map(data, (value, index) => (
              <th className="region-name" colSpan={data[index].length}>
                {value[0].title}
              </th>
            ))}
          </tr>
        )}
        {!collapsed && (
          <tr className="latency-table__row">
            {multipleHeader && <td />}
            <td className="latency-table__cell latency-table__cell--spacer" />
            {_.map(data, (value) =>
              _.map(value, (identity, index: number) =>
                createHeaderCell(
                  staleIDs,
                  identity,
                  `0-${value.nodeID}`,
                  index === 0,
                  collapsed,
                ),
              ),
            )}
          </tr>
        )}
      </thead>
      <tbody>
        {_.map(data, (value, index) =>
          _.map(data[index], (identityA, indA: number) => {
            return (
              <tr
                key={index}
                className={`latency-table__row ${
                  data[index].length === indA + 1
                    ? "latency-table__row--end"
                    : ""
                }`}
              >
                {multipleHeader && Number(indA) === 0 && (
                  <th rowSpan={collapsed ? 1 : data[index][0].rowCount}>
                    {value[0].title}
                  </th>
                )}
                {createHeaderCell(
                  staleIDs,
                  identityA,
                  `${identityA.nodeID}-0`,
                  false,
                  collapsed,
                )}
                {_.map(identityA.row, (identity: any, indexB: number) =>
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
        )}
      </tbody>
    </table>
  );
};
