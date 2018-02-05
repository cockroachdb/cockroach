import _ from "lodash";
import React, { Component } from "react";
import {
  TreeNode, TreePath, layoutTree, flatten, sumValuesUnderPaths,
  deepIncludes,
} from "./tree";
import classNames from "classnames";
import "./Matrix.styl";

const DOWN_ARROW = "▼";
const SIDE_ARROW = "▶";

interface MatrixState {
  collapsedRows: TreePath[];
  collapsedCols: TreePath[];
}

interface MatrixProps<R, C> {
  label?: JSX.Element;
  cols: TreeNode<C>;
  rows: TreeNode<R>;
  getValue: (rowPath: TreePath, colPath: TreePath) => number;
  rowLeafLabel?: (row: R, path?: TreePath) => string;
  rowNodeLabel?: (row: R, path?: TreePath) => string;
  colLeafLabel?: (col: C, path: TreePath, isPlaceholder: boolean) => string;
  colNodeLabel?: (col: C, path: TreePath, isPlaceholder: boolean) => string;
}

class Matrix<R, C> extends Component<MatrixProps<R, C>, MatrixState> {

  constructor() {
    super();
    this.state = {
      collapsedRows: [],
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

  render() {
    const {
      label,
      cols,
      rows,
      getValue,
      colLeafLabel,
      colNodeLabel,
      rowLeafLabel,
      rowNodeLabel,
    } = this.props;
    const {
      collapsedRows,
      collapsedCols,
    } = this.state;

    const flattenedRows = flatten(rows, collapsedRows, true /* includeNodes */);
    const headerRows = layoutTree(cols, collapsedCols);
    const flattenedCols = flatten(cols, collapsedCols, false /* includeNodes */);

    return (
      <table className="replica-matrix">
        <thead>
        {headerRows.slice(1).map((row, idx) => (
          <tr key={idx}>
            {idx === 0
              ? <th>{label}</th>
              : <th />}
            {row.map((col) => {
              const colIsCollapsed = deepIncludes(collapsedCols, col.path);
              const arrow = colIsCollapsed ? SIDE_ARROW : DOWN_ARROW;
              return (
                <th
                  key={col.path.join("/")}
                  colSpan={col.width}
                  className={classNames("matrix-column-header", { toggleable: col.depth > 1 })}
                  onClick={() => (
                    colIsCollapsed
                      ? this.expandCol(col.path)
                      : this.collapseCol(col.path)
                  )}
                >
                  {col.isPlaceholder
                    ? null
                    : col.depth === 1
                      ? colLeafLabel(col.data, col.path, col.isPlaceholder)
                      : `${arrow} ${colNodeLabel(col.data, col.path, col.isPlaceholder)}`}
                </th>
              );
            })}
          </tr>
        ))}
        </thead>
        <tbody>
        {flattenedRows.filter((n) => n.depth > 0).map((row) => {
          const arrow = row.isCollapsed ? SIDE_ARROW : DOWN_ARROW;
          return (
            <tr
              key={JSON.stringify(row)}
              className={classNames("matrix-row", { node: !row.isLeaf })}
              onClick={() => (
                row.isCollapsed
                  ? this.expandRow(row.path)
                  : this.collapseRow(row.path)
              )}
            >
              <th
                style={{
                  paddingLeft: (row.depth - 1) * 30 + 5,
                  textAlign: "left",
                  fontWeight: row.isLeaf ? "normal" : "bold",
                }}
              >
                {row.isLeaf
                  ? rowLeafLabel(row.data, row.path)
                  : `${arrow} ${rowNodeLabel(row.data, row.path)}`}
              </th>
              {flattenedCols.map((col) => {
                return (
                  <td key={col.path.join("/")} style={{textAlign: "right"}}>
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

export default Matrix;
