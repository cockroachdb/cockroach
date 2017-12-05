import _ from "lodash";
import React, { Component } from "react";
import {TreeNode, TreePath, layoutTree, getLeaves, flatten} from "./tree";
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
  renderCell: (row: R, col: C) => JSX.Element | null;
  rowLeafLabel?: ((row: R, path: TreePath) => string) | ((row: R) => string);
  rowNodeLabel?: ((row: R, path: TreePath) => string) | ((row: R) => string);
  colLeafLabel?: ((col: C, path: TreePath) => string) | ((col: C) => string);
  colNodeLabel?: ((col: C, path: TreePath) => string) | ((col: C) => string);
}

class Matrix<R, C> extends Component<MatrixProps<R, C>, MatrixState> {

  constructor() {
    super();
    this.state = {
      collapsedRows: [],
      collapsedCols: [],
    };
  }

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

  render() {
    const {
      label,
      cols,
      rows,
      renderCell,
    } = this.props;
    const {
      collapsedRows,
    } = this.state;

    const colNodeLabel = this.props.colNodeLabel || ((col: C) => (<span>{JSON.stringify(col)}</span>));
    const colLeafLabel = this.props.colLeafLabel || ((col: C) => (<span>{JSON.stringify(col)}</span>));
    const rowNodeLabel = this.props.rowNodeLabel || ((row: R) => (<span>{JSON.stringify(row)}</span>));
    const rowLeafLabel = this.props.rowLeafLabel || ((row: R) => (<span>{JSON.stringify(row)}</span>));

    const handleCollapseRow = this._handleCollapseRow.bind(this);
    const handleUnCollapseRow = this._handleUnCollapseRow.bind(this);

    const flattenedRows = flatten(rows, collapsedRows);
    const headerRows = layoutTree(cols);
    const flattenedCols = getLeaves(cols);

    return (
      <table className="replica-matrix">
        <thead>
        {headerRows.slice(1).map((row, idx) => (
          <tr key={idx}>
            {idx === 0
              ? <th>{label}</th>
              : <th />}
            {row.map((col) => (
              <th
                key={col.path.join("/")}
                colSpan={col.width}
                style={{fontWeight: "bold"}}
              >
                {col.depth === 1
                  ? colLeafLabel(col.data, col.path)
                  : colNodeLabel(col.data, col.path)}
              </th>
            ))}
          </tr>
        ))}
        </thead>
        <tbody>
        {flattenedRows.filter((n) => n.depth > 0).map((row) => {
          const rowIsCollapsed = collapsedRows.filter((p) => _.isEqual(p, row.path)).length > 0;
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
              {flattenedCols.map((col, idx) => {
                return (
                  <td key={idx} style={{textAlign: "right"}}>
                    {renderCell(row.data, col.data)}
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

export default Matrix;
