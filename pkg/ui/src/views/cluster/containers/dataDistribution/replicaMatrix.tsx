import d3 from "d3";
import _ from "lodash";
import React, { Component } from "react";
import { Link } from "react-router";
import classNames from "classnames";

import {
  TreeNode,
  TreePath,
  layoutTreeHorizontal,
  flatten,
  sumValuesUnderPaths,
  LayoutCell,
  FlattenedNode, visitNodes, PaginationState,
} from "./tree";
import { cockroach } from "src/js/protos";
import INodeDescriptor = cockroach.roachpb.INodeDescriptor;
import "./replicaMatrix.styl";
import { createSelector } from "reselect";
import {
  AssocList,
  getAssocList,
  putAssocList,
} from "src/views/cluster/containers/dataDistribution/assocList";

const DOWN_ARROW = "▼";
const SIDE_ARROW = "▶";

const PAGE_SIZE = 10;

interface ReplicaMatrixState {
  collapsedRows: TreePath[];
  collapsedCols: TreePath[];
  selectedMetric: string;
  paginatedPaths: AssocList<TreePath, PaginationState>;
}

interface ReplicaMatrixProps {
  cols: TreeNode<INodeDescriptor>;
  rows: TreeNode<SchemaObject>;
  getValue: (metric: string) => (rowPath: TreePath, colPath: TreePath) => number;
}

// Amount to indent for a row each level of depth in the tree.
const ROW_TREE_INDENT_PX = 18;
// Margin for all rows in the matrix. Strangely, <th>s can't have margins
// applied in CSS.
const ROW_LEFT_MARGIN_PX = 5;

export const METRIC_REPLICAS = "REPLICAS";
export const METRIC_LEASEHOLDERS = "LEASEHOLDERS";
export const METRIC_QPS = "QPS";

class ReplicaMatrix extends Component<ReplicaMatrixProps, ReplicaMatrixState> {

  constructor(props: ReplicaMatrixProps) {
    super(props);

    const collapsedPaths = [
      ["system"],
      ["defaultdb"],
      ["postgres"],
    ];
    visitNodes(props.rows, (node, path) => {
      if (node.data.tableName) { // [db, table]
        collapsedPaths.push(path);
        return false;
      }
      return true;
    });

    // TODO(vilterp): put all this state in the URL
    this.state = {
      collapsedRows: collapsedPaths,
      collapsedCols: [],
      selectedMetric: METRIC_REPLICAS,
      paginatedPaths: [],
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

  colLabel(col: LayoutCell<INodeDescriptor>) {
    if (col.isPlaceholder) {
      return null;
    }

    if (col.isLeaf) {
      return <CustomLink to={`/node/${col.data.node_id}`}>n{col.data.node_id}</CustomLink>;
    }

    const arrow = col.isCollapsed ? SIDE_ARROW : DOWN_ARROW;
    const localityLabel = col.path.length === 0 ? "Cluster" : col.path[col.path.length - 1];
    return <span>{arrow} {localityLabel}</span>;
  }

  rowLabel(row: FlattenedNode<SchemaObject>) {
    const data = row.node.data;

    if (data.rangeID) {
      return (
        <CustomLink to={`/reports/range/${data.rangeID}`}>
          r{data.rangeID}
        </CustomLink>
      );
    }

    if (data.tableName) {
      return (
        <CustomLink to={`/database/${data.dbName}/table/${data.tableName}`}>
          {data.tableName}
        </CustomLink>
      );
    }

    return data.dbName ? `DB: ${data.dbName}` : "Cluster";
  }

  rowLabelAndArrow(row: FlattenedNode<SchemaObject>) {
    const label = this.rowLabel(row);
    const arrow = row.isCollapsed ? SIDE_ARROW : DOWN_ARROW;

    if (row.isLeaf) {
      return label;
    } else {
      return <span>{arrow} {label}</span>;
    }
  }

  handleChangeMetric = (evt: React.FormEvent<HTMLSelectElement>) => {
    this.setState({
      selectedMetric: evt.currentTarget.value,
    });
  }

  renderMetricSelector() {
    return (
      <select value={this.state.selectedMetric} onChange={this.handleChangeMetric}>
        <option value={METRIC_REPLICAS}># Replicas</option>
        <option value={METRIC_LEASEHOLDERS}># Leaseholders</option>
        <option value={METRIC_QPS}>QPS</option>
      </select>
    );
  }

  renderCell(
    row: FlattenedNode<SchemaObject>,
    col: FlattenedNode<INodeDescriptor>,
    scale: d3.scale.Linear<number, number>,
    getValue: (rowPath: TreePath, colPath: TreePath) => number,
  ) {
    if (!(row.isLeaf || row.isCollapsed)) {
      return null;
    }

    const value = sumValuesUnderPaths(
      this.props.rows, this.props.cols, row.path, col.path, getValue,
    );

    if (value === 0) {
      return null;
    }

    const lightnessValue = scale(value);
    const backgroundColor = `hsl(210, 100%, ${lightnessValue}%)`;
    const textColor = lightnessValue < 75 ? "white" : "black";

    return (
      <div
        className="matrix__cell-value"
        style={{ backgroundColor: backgroundColor, color: textColor }}
      >
        {value}
      </div>
    );
  }

  handleChangePage = (rowPath: TreePath, delta: number) => {
    this.setState({
      paginatedPaths: putAssocList(this.state.paginatedPaths, rowPath, (ps) => {
        if (ps) {
          return {
            ...ps,
            page: ps.page + delta,
          };
        }
        return {
          page: 1, // paging to the right for the first time
          path: rowPath,
          sortDesc: true,
        };
      }),
    });
  }

  render() {
    const {
      cols,
    } = this.props;
    const {
      collapsedCols,
    } = this.state;

    const propsAndState = {
      props: this.props,
      state: this.state,
    };

    const flattenedRows = selectFlattenedRows(propsAndState);
    const headerRows = layoutTreeHorizontal(cols, collapsedCols);
    const flattenedCols = selectFlattenedCols(propsAndState);

    const getValue = selectGetValueFun(propsAndState);
    const scale = selectScale(propsAndState);

    return (
      <table className="matrix">
        <thead>
          {headerRows.map((row, idx) => (
            <tr key={idx}>
              {idx === 0
                ? <th className="matrix__metric-label">{this.renderMetricSelector()}</th>
                : <th />}
              {row.map((col) => (
                <th
                  key={col.path.join("/")}
                  colSpan={col.width}
                  className={classNames(
                    "matrix__column-header",
                    { "matrix__column-header--internal-node": !(col.isLeaf || col.isPlaceholder) },
                  )}
                  onClick={() => (
                    col.isCollapsed
                      ? this.expandCol(col.path)
                      : this.collapseCol(col.path)
                  )}
                >
                  {this.colLabel(col)}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {flattenedRows.map((row) => {
            const paginationState = getAssocList(this.state.paginatedPaths, row.path);
            const page = paginationState ? paginationState.page : 0;
            const children = row.node.children;
            const numChildren = children ? children.length : 0;
            const numPages = Math.ceil(numChildren / PAGE_SIZE);

            return [
              <tr
                key={row.path.join("/")}
                className={classNames(
                  "matrix__row",
                  { "matrix__row--internal-node": !row.isLeaf },
                )}
                onClick={() => (
                  row.isCollapsed
                    ? this.expandRow(row.path)
                    : this.collapseRow(row.path)
                )}
              >
                <th
                  className={classNames(
                    "matrix__row-header",
                    { "matrix__row-header--internal-node": !row.isLeaf },
                  )}
                  style={{ paddingLeft: row.depth * ROW_TREE_INDENT_PX + ROW_LEFT_MARGIN_PX }}
                >
                  {this.rowLabelAndArrow(row)}
                </th>
                {flattenedCols.map((col) => {
                  return (
                    <td
                      key={col.path.join("/")}
                      className="matrix__cell"
                    >
                      {this.renderCell(row, col, scale, getValue)}
                    </td>
                  );
                })}
              </tr>,
              (row.isPaginated && !(row.isCollapsed || row.isLeaf))
                ? <tr>
                    <td />
                    <td colSpan={flattenedCols.length} style={{ padding: 5 }}>
                      <button
                        onClick={() => this.handleChangePage(row.path, -1)}
                        disabled={page === 0}
                      >
                        &lt; Prev
                      </button>
                      {" "}
                      {page + 1} out of {numPages}
                      {" "}
                      <button
                        onClick={() => this.handleChangePage(row.path, 1)}
                        disabled={page === numPages - 1}
                      >
                        Next &gt;
                      </button>
                    </td>
                  </tr>
                : null,
            ];
          })}
        </tbody>
      </table>
    );
  }

}

function CustomLink(props: { to: string, children: React.ReactNode }) {
  return (
    <Link to={props.to} target="_blank" onClick={(evt) => evt.stopPropagation()}>
      {props.children}
    </Link>
  );
}

// Selectors

interface PropsAndState {
  props: ReplicaMatrixProps;
  state: ReplicaMatrixState;
}

const selectFlattenedRows = createSelector(
  (propsAndState: PropsAndState) => propsAndState.props.rows,
  (propsAndState: PropsAndState) => propsAndState.state.collapsedRows,
  (propsAndState: PropsAndState) => propsAndState.state.paginatedPaths,
  (
    rows: TreeNode<SchemaObject>,
    collapsedRows: TreePath[],
    paginationStates: AssocList<TreePath, PaginationState>,
  ) => {
    console.log("flattening rows");
    return flatten(rows, collapsedRows, true /* includeNodes */, paginationStates, PAGE_SIZE);
  },
);

const selectFlattenedCols = createSelector(
  (propsAndState: PropsAndState) => propsAndState.props.cols,
  (propsAndState: PropsAndState) => propsAndState.state.collapsedCols,
  (cols: TreeNode<INodeDescriptor>, collapseCols: TreePath[]) => {
    console.log("flattening cols");
    return flatten(cols, collapseCols, false /* includeNodes */);
  },
);

const selectGetValueFun = createSelector(
  (propsAndState: PropsAndState) => propsAndState.state.selectedMetric,
  (propsAndState: PropsAndState) => propsAndState.props.getValue,
  (
    selectedMetric: string,
    getValue: (metric: string) => (rowPath: TreePath, colPath: TreePath) => number,
  ) => {
    return getValue(selectedMetric);
  },
);

const selectScale = createSelector(
  (propsAndState: PropsAndState) => propsAndState.props.rows,
  (propsAndState: PropsAndState) => propsAndState.props.cols,
  selectGetValueFun,
  selectFlattenedRows,
  selectFlattenedCols,
  (
    rows: TreeNode<SchemaObject>,
    cols: TreeNode<INodeDescriptor>,
    getValue: (rowPath: TreePath, colPath: TreePath) => number,
    flattenedRows: FlattenedNode<SchemaObject>[],
    flattenedCols: FlattenedNode<INodeDescriptor>[],
  ) => {
    console.log("computing scale");
    const allVals: number[] = [];
    flattenedRows.forEach((row) => {
      flattenedCols.forEach((col) => {
        if (!(row.isLeaf || row.isCollapsed)) {
          return;
        }
        const value = sumValuesUnderPaths(rows, cols, row.path, col.path, getValue);
        allVals.push(value);
      });
    });

    const extent = d3.extent(allVals);
    return d3.scale.linear()
      .domain([0, extent[1]])
      .range([100, 50]); // TODO(vilterp): factor these out into constants
  },
);

export default ReplicaMatrix;

export interface SchemaObject {
  dbName?: string;
  tableName?: string;
  tableID?: number;
  rangeID?: string;
}
