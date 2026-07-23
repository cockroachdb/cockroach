// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import classNames from "classnames";
import isEqual from "lodash/isEqual";
import React, { useState } from "react";

import { cockroach, google } from "src/js/protos";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

import {
  TreeNode,
  TreePath,
  layoutTreeHorizontal,
  flatten,
  sumValuesUnderPaths,
  LayoutCell,
  FlattenedNode,
} from "./tree";

import NodeDescriptor$Properties = cockroach.roachpb.INodeDescriptor;
import ITimestamp = google.protobuf.ITimestamp;

import "./replicaMatrix.scss";

const DOWN_ARROW = "▼";
const SIDE_ARROW = "▶";

interface ReplicaMatrixProps {
  cols: TreeNode<NodeDescriptor$Properties>;
  rows: TreeNode<SchemaObject>;
  getValue: (rowPath: TreePath, colPath: TreePath) => number;
}

// Amount to indent for a row each level of depth in the tree.
const ROW_TREE_INDENT_PX = 18;
// Margin for all rows in the matrix. Strangely, <th>s can't have margins
// applied in CSS.
const ROW_LEFT_MARGIN_PX = 5;

// Default databases collapsed on initial render.
const INITIAL_COLLAPSED_ROWS: TreePath[] = [
  ["system"],
  ["defaultdb"],
  ["postgres"],
];

function colLabel(col: LayoutCell<NodeDescriptor$Properties>): string {
  if (col.isPlaceholder) {
    return null;
  }
  if (col.isLeaf) {
    return `n${col.data.node_id}`;
  }
  const arrow = col.isCollapsed ? SIDE_ARROW : DOWN_ARROW;
  const localityLabel =
    col.path.length === 0 ? "Cluster" : col.path[col.path.length - 1];
  return `${arrow} ${localityLabel}`;
}

function rowLabelText(row: FlattenedNode<SchemaObject>): string {
  if (row.isLeaf) {
    return row.data.tableName;
  }
  const arrow = row.isCollapsed ? SIDE_ARROW : DOWN_ARROW;
  const label = row.data.dbName ? `DB: ${row.data.dbName}` : "Cluster";
  return `${arrow} ${label}`;
}

function rowLabel(row: FlattenedNode<SchemaObject>): React.ReactNode {
  const text = rowLabelText(row);
  const label = (
    <span
      className={classNames("table-label", {
        "table-label--dropped": !!row.data.droppedAt,
      })}
    >
      {text}
    </span>
  );

  if (row.data.droppedAt) {
    return (
      <ToolTipWrapper
        text={
          <span>
            Dropped at {util.TimestampToMoment(row.data.droppedAt).format()}.
            Will eventually be garbage collected according to this schema
            object's GC TTL.
          </span>
        }
      >
        {label}
      </ToolTipWrapper>
    );
  }

  return label;
}

function ReplicaMatrix({
  cols,
  rows,
  getValue,
}: ReplicaMatrixProps): React.ReactElement {
  const [collapsedRows, setCollapsedRows] = useState<TreePath[]>(
    INITIAL_COLLAPSED_ROWS,
  );
  const [collapsedCols, setCollapsedCols] = useState<TreePath[]>([]);

  const expandRow = (path: TreePath) => {
    setCollapsedRows(prev => prev.filter(tp => !isEqual(tp, path)));
  };

  const collapseRow = (path: TreePath) => {
    setCollapsedRows(prev => [...prev, path]);
  };

  const expandCol = (path: TreePath) => {
    setCollapsedCols(prev => prev.filter(tp => !isEqual(tp, path)));
  };

  const collapseCol = (path: TreePath) => {
    setCollapsedCols(prev => [...prev, path]);
  };

  const flattenedRows = flatten(rows, collapsedRows, true /* includeNodes */);
  const headerRows = layoutTreeHorizontal(cols, collapsedCols);
  const flattenedCols = flatten(cols, collapsedCols, false /* includeNodes */);

  return (
    <table className="matrix">
      <thead>
        {headerRows.map((row, idx) => (
          <tr key={idx}>
            {idx === 0 ? (
              <th className="matrix__metric-label"># Replicas</th>
            ) : (
              <th />
            )}
            {row.map(col => (
              <th
                key={col.path.join("/")}
                colSpan={col.width}
                className={classNames("matrix__column-header", {
                  "matrix__column-header--internal-node": !(
                    col.isLeaf || col.isPlaceholder
                  ),
                })}
                onClick={() =>
                  col.isCollapsed ? expandCol(col.path) : collapseCol(col.path)
                }
              >
                {colLabel(col)}
              </th>
            ))}
          </tr>
        ))}
      </thead>
      <tbody>
        {flattenedRows.map(row => {
          return (
            <tr
              key={row.path.join("/")}
              className={classNames("matrix__row", {
                "matrix__row--internal-node": !row.isLeaf,
              })}
              onClick={() =>
                row.isCollapsed ? expandRow(row.path) : collapseRow(row.path)
              }
            >
              <th
                className={classNames("matrix__row-header", {
                  "matrix__row-header--internal-node": !row.isLeaf,
                })}
                style={{
                  paddingLeft:
                    row.depth * ROW_TREE_INDENT_PX + ROW_LEFT_MARGIN_PX,
                }}
              >
                {rowLabel(row)}
              </th>
              {flattenedCols.map(col => {
                return (
                  <td key={col.path.join("/")} className="matrix__cell-value">
                    {row.isLeaf || row.isCollapsed
                      ? emptyIfZero(
                          sumValuesUnderPaths(
                            rows,
                            cols,
                            row.path,
                            col.path,
                            getValue,
                          ),
                        )
                      : null}
                  </td>
                );
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function emptyIfZero(n: number): string {
  if (n === 0) {
    return "";
  }
  return `${n}`;
}

export default ReplicaMatrix;

export interface SchemaObject {
  dbName?: string;
  tableName?: string;
  droppedAt?: ITimestamp;
}
