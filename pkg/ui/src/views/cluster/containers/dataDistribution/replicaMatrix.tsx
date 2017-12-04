import _ from "lodash";
import React, { Component } from "react";
import classNames from "classnames";

import {
  TreeNode,
  TreePath,
  layoutTree,
  flatten,
  sumValuesUnderPaths,
  deepIncludes,
  LayoutNode,
  FlattenedNode,
} from "./tree";
import { cockroach } from "src/js/protos";
import NodeDescriptor$Properties = cockroach.roachpb.NodeDescriptor$Properties;
import "./replicaMatrix.styl";

const DOWN_ARROW = "▼";
const SIDE_ARROW = "▶";

interface ReplicaMatrixState {
  collapsedRows: TreePath[];
  collapsedCols: TreePath[];
}

interface ReplicaMatrixProps {
  cols: TreeNode<NodeDescriptor$Properties>;
  rows: TreeNode<SchemaObject>;
  getValue: (rowPath: TreePath, colPath: TreePath) => number;
}

const ROW_TREE_INDENT_PX = 18;

class ReplicaMatrix extends Component<ReplicaMatrixProps, ReplicaMatrixState> {

  constructor(props: ReplicaMatrixProps) {
    super(props);
    this.state = {
      collapsedRows: [["system"]],
      collapsedCols: [],
    };
  }

  expandRow = (path: TreePath) => {
    this.setState({
      collapsedRows: this.state.collapsedRows.filter((tp) => !_.isEqual(tp, path)),
    });
  }

  collapseRow = (path: TreePath) => {
    this.setState({
      collapsedRows: [...this.state.collapsedRows, path],
    });
  }

  expandCol = (path: TreePath) => {
    this.setState({
      collapsedCols: this.state.collapsedCols.filter((tp) => !_.isEqual(tp, path)),
    });
  }

  collapseCol = (path: TreePath) => {
    this.setState({
      collapsedCols: [...this.state.collapsedCols, path],
    });
  }

  colLabel(col: LayoutNode<NodeDescriptor$Properties>, colIsCollapsed: boolean): string {
    if (col.isPlaceholder) {
      return null;
    }

    if (col.depth === 1) {
      return `n${col.data.node_id}`;
    }

    const arrow = colIsCollapsed ? SIDE_ARROW : DOWN_ARROW;
    const localityLabel = col.path.length === 0 ? "Cluster" : col.path[col.path.length - 1];
    return `${arrow} ${localityLabel}`;
  }

  rowLabel(row: FlattenedNode<SchemaObject>): string {
    if (row.isLeaf) {
      return row.data.tableName;
    }

    const arrow = row.isCollapsed ? SIDE_ARROW : DOWN_ARROW;
    const label = row.data ? `DB: ${row.data.dbName}` : "Cluster";
    return `${arrow} ${label}`;
  }

  render() {
    const {
      cols,
      rows,
      getValue,
    } = this.props;
    const {
      collapsedRows,
      collapsedCols,
    } = this.state;

    const flattenedRows = flatten(rows, collapsedRows, true /* includeNodes */);
    const headerRows = layoutTree(cols, collapsedCols);
    const flattenedCols = flatten(cols, collapsedCols, false /* includeNodes */);

    return (
      <table className="matrix">
        <thead>
          {headerRows.map((row, idx) => (
            <tr key={idx}>
              {idx === 0
                ? <th className="matrix__metric-label"># Replicas</th>
                : <th />}
              {row.map((col) => {
                const colIsCollapsed = deepIncludes(collapsedCols, col.path);
                return (
                  <th
                    key={col.path.join("/")}
                    colSpan={col.width}
                    className={classNames(
                      "matrix__column-header",
                      { "matrix__column-header--node": col.depth > 1 },
                    )}
                    onClick={() => (
                      colIsCollapsed
                        ? this.expandCol(col.path)
                        : this.collapseCol(col.path)
                    )}
                  >
                    {this.colLabel(col, colIsCollapsed)}
                  </th>
                );
              })}
            </tr>
          ))}
        </thead>
        <tbody>
          {flattenedRows.map((row) => {
            return (
              <tr
                key={JSON.stringify(row)}
                className={classNames("matrix__row", { "matrix__row--node": !row.isLeaf })}
                onClick={() => (
                  row.isCollapsed
                    ? this.expandRow(row.path)
                    : this.collapseRow(row.path)
                )}
              >
                <th
                  className={classNames(
                    "matrix__row-label",
                    { "matrix__row-label--node": !row.isLeaf },
                  )}
                  style={{ paddingLeft: row.depth * ROW_TREE_INDENT_PX + 5 }}
                >
                  {this.rowLabel(row)}
                </th>
                {flattenedCols.map((col) => {
                  return (
                    <td
                      key={col.path.join("/")}
                      className="matrix__cell-value"
                    >
                      {row.isLeaf || row.isCollapsed
                        ? emptyIfZero(sumValuesUnderPaths(rows, cols, row.path, col.path, getValue))
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
}
