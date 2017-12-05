import React, { Component } from "react";
import { TreeNode, layoutTree, getLeaves, flatten } from "./tree";
// import './Matrix.css';

const DOWN_ARROW = "▼";

interface MatrixProps<R, C> {
  label?: JSX.Element;
  cols: TreeNode<C>;
  rows: TreeNode<R>;
  renderCell: (row: R, col: C) => JSX.Element | null;
  rowLeafLabel?: (row: R) => string;
  rowNodeLabel?: (row: R) => string;
  colLeafLabel?: (col: C) => string;
  colNodeLabel?: (col: C) => string;
}

class Matrix<R, C> extends Component<MatrixProps<R, C>, {}> {

  render() {
    const {
      label,
      cols,
      rows,
      renderCell,
    } = this.props;

    const colNodeLabel = this.props.colNodeLabel || ((col: C) => (<span>{JSON.stringify(col)}</span>));
    const colLeafLabel = this.props.colLeafLabel || ((col: C) => (<span>{JSON.stringify(col)}</span>));
    const rowNodeLabel = this.props.rowNodeLabel || ((row: R) => (<span>{JSON.stringify(row)}</span>));
    const rowLeafLabel = this.props.rowLeafLabel || ((row: R) => (<span>{JSON.stringify(row)}</span>));

    const flattenedRows = flatten(rows);
    const headerRows = layoutTree(cols);
    const flattenedCols = getLeaves(cols);

    return (
      <table className="replica-matrix">
        <thead>
        {headerRows.map((row, idx) => (
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
                {col.depth === 1 ? colLeafLabel(col.data) : colNodeLabel(col.data)}
              </th>
            ))}
          </tr>
        ))}
        </thead>
        <tbody>
        {flattenedRows.filter((n) => n.depth > 0).map((row) => (
          <tr key={JSON.stringify(row)}>
            <th
              style={{
                paddingLeft: (row.depth - 1) * 30,
                textAlign: "left",
                fontWeight: row.isLeaf ? "normal" : "bold",
              }}
            >
              {row.isLeaf
                ? rowLeafLabel(row.data)
                : `${DOWN_ARROW} ${rowNodeLabel(row.data)}`}
            </th>
            {flattenedCols.map((col, idx) => {
              return (
                <td key={idx} style={{textAlign: "right"}}>
                  {renderCell(row.data, col.data)}
                </td>
              );
            })}
          </tr>
        ))}
        </tbody>
      </table>
    );
  }

}

export default Matrix;
