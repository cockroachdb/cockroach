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

  // TODO(vilterp): DRY these up somehow

  _handleUnCollapseRow(path: TreePath) {
    this.setState({
      collapsedRows: this.state.collapsedRows.filter((tp) => !_.isEqual(tp, path)),
    });
  }

  _handleCollapseRow(path: TreePath) {
    this.setState({
      collapsedRows: [...this.state.collapsedRows, path],
    });
  }

  _handleUnCollapseCol(path: TreePath) {
    this.setState({
      collapsedCols: this.state.collapsedCols.filter((tp) => !_.isEqual(tp, path)),
    });
  }

  _handleCollapseCol(path: TreePath) {
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

    // TODO(vilterp): bind these in the constructor or something
    // shouldn't do this here every frame
    const handleCollapseRow = this._handleCollapseRow.bind(this);
    const handleUnCollapseRow = this._handleUnCollapseRow.bind(this);
    const handleCollapseCol = this._handleCollapseCol.bind(this);
    const handleUnCollapseCol = this._handleUnCollapseCol.bind(this);

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
                  style={{fontWeight: "bold"}}
                  className={classNames("matrix-column-header", { toggleable: col.depth > 1 })}
                  onClick={() => (
                    colIsCollapsed
                      ? handleUnCollapseCol(col.path)
                      : handleCollapseCol(col.path)
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
          const rowIsCollapsed = deepIncludes(collapsedRows, row.path);
          const arrow = rowIsCollapsed ? SIDE_ARROW : DOWN_ARROW;
          return (
            <tr
              key={JSON.stringify(row)}
              className={classNames("matrix-row", { node: !row.isLeaf })}
              onClick={() => (
                rowIsCollapsed
                  ? handleUnCollapseRow(row.path)
                  : handleCollapseRow(row.path)
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
                    {row.isLeaf || rowIsCollapsed
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
