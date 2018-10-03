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
  LayoutCell,
  FlattenedNode,
  visitNodes,
  PaginationState,
  SortState,
  isLeaf,
  augmentWithSize,
  TreeWithSize,
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
import { Bytes } from "src/util/format";

const DOWN_ARROW = "▼";
const SIDE_ARROW = "▶";

const PAGE_SIZE = 10;

interface ReplicaMatrixState {
  collapsedRows: TreePath[];
  collapsedCols: TreePath[];
  selectedMetric: string;
  paginationStates: AssocList<TreePath, PaginationState>;
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
export const METRIC_LIVE_BYTES = "LIVE_BYTES";

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
      paginationStates: [],
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
        <option value={METRIC_LIVE_BYTES}>Size (Live Bytes)</option>
      </select>
    );
  }

  formatValue(value: number) {
    switch (this.state.selectedMetric) {
      case METRIC_LIVE_BYTES:
        return Bytes(value);
      default:
        return value;
    }
  }

  renderCell(
    row: FlattenedNode<SchemaObject>,
    scale: d3.scale.Linear<number, number>,
    value: number,
  ) {
    if (!(row.isLeaf || row.isCollapsed)) {
      return null;
    }

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
        {this.formatValue(value)}
      </div>
    );
  }

  handleChangePage = (rowPath: TreePath, delta: number) => {
    this.setState({
      paginationStates: putAssocList(this.state.paginationStates, rowPath, (ps) => {
        if (ps) {
          return {
            ...ps,
            page: ps.page + delta,
          };
        }
        return {
          page: 1, // paging to the right for the first time
          path: rowPath,
          sortState: SortState.NONE,
        };
      }),
    });
  }

  handleChangeSortState = (rowPath: TreePath, newState: SortState) => {
    this.setState({
      paginationStates: putAssocList(this.state.paginationStates, rowPath, (ps) => {
        if (ps) {
          return {
            ...ps,
            sortState: newState,
          };
        }
        return {
          page: 0,
          path: rowPath,
          sortState: newState,
        };
      }),
    });
  }

  renderPager(row: FlattenedNode<SchemaObject>, numFlattenedCols: number) {
    const paginationState = getAssocList(this.state.paginationStates, row.path);
    const page = paginationState ? paginationState.page : 0;
    const children = row.node.children;
    const numChildren = children ? children.length : 0;
    const numPages = Math.ceil(numChildren / PAGE_SIZE);

    return (
      <tr>
        <td />
        <td colSpan={numFlattenedCols} style={{ padding: 5 }}>
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
          <SortStateSelector
            state={paginationState ? paginationState.sortState : SortState.NONE}
            onChange={(newSortState) => this.handleChangeSortState(row.path, newSortState)}
          />
        </td>
      </tr>
    );
  }

  render() {
    console.log("======== ReplicaMatrix render ========");

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

    const { allVals: valuesArray } = selectAllVals(propsAndState);
    const scale = selectScale(propsAndState);

    const masterGrid = selectMasterGrid(propsAndState);
    console.log({ masterGrid });

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
          {flattenedRows.map((row, rowIdx) => {
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
                {flattenedCols.map((col, colIdx) => {
                  return (
                    <td
                      key={col.path.join("/")}
                      className="matrix__cell"
                    >
                      {this.renderCell(row, scale, valuesArray[rowIdx][colIdx])}
                    </td>
                  );
                })}
              </tr>,
              (row.isPaginated && !(row.isCollapsed || row.isLeaf))
                ? this.renderPager(row, flattenedCols.length)
                : null,
            ];
          })}
        </tbody>
      </table>
    );
  }

}

// TODO(vilterp): just use a radio button?
function SortStateSelector(props: { state: SortState, onChange: (sortState: SortState) => void }) {
  function option(val: SortState) {
    return (
      <span
        className={classNames(
          "sort-state-selector__option",
          { "sort-state-selector__option--selected": val === props.state },
        )}
        onClick={() => props.onChange(val)}
      >
        {val}
      </span>
    );
  }
  return (
    <span className="sort-state-selector">
      {option(SortState.ASC)}
      {option(SortState.DESC)}
      {option(SortState.NONE)}
    </span>
  );
}

function CustomLink(props: { to: string, children: React.ReactNode }) {
  return (
    <Link to={props.to} target="_blank" onClick={(evt) => evt.stopPropagation()}>
      {props.children}
    </Link>
  );
}

// SELECTORS

interface PropsAndState {
  props: ReplicaMatrixProps;
  state: ReplicaMatrixState;
}

const selectGetValueFun = createSelector(
  (propsAndState: PropsAndState) => propsAndState.state.selectedMetric,
  (propsAndState: PropsAndState) => propsAndState.props.getValue,
  (
    selectedMetric: string,
    getValue: (metric: string) => (rowPath: TreePath, colPath: TreePath) => number,
  ) => {
    console.log("computing selectGetValueFun");
    return getValue(selectedMetric);
  },
);

const selectRowsWithSize = createSelector(
  (propsAndState: PropsAndState) => propsAndState.props.rows,
  (rows: TreeNode<SchemaObject>) => {
    return augmentWithSize(rows);
  },
);

const selectColsWithSize = createSelector(
  (propsAndState: PropsAndState) => propsAndState.props.cols,
  (cols: TreeNode<INodeDescriptor>) => {
    return augmentWithSize(cols);
  },
);

// TODO(vilterp): make this depend on the master grid
interface SumAndIncrease {
  sum: number;
  increase: number;
}

const selectMasterGrid = createSelector(
  selectRowsWithSize,
  selectColsWithSize,
  selectGetValueFun,
  function makeMasterGrid(
    rows: TreeWithSize<SchemaObject>,
    cols: TreeWithSize<INodeDescriptor>,
    getValue: (rowPath: TreePath, colPath: TreePath) => number,
  ) {
    console.log("computing master grid");
    // TODO(vilterp): build this as we go
    const outputRows: number[][] = [];
    for (let i = 0; i < rows.size; i++) {
      const row: number[] = [];
      for (let j = 0; j < cols.size; j++) {
        row.push(0);
      }
      outputRows.push(row);
    }

    function visitRows(rowPath: TreePath, rowIdx: number, row: TreeWithSize<SchemaObject>): SumAndIncrease {
      // console.log("visitRows", rowPath, rowIdx);

      function visitCols(colPath: TreePath, colIdx: number, col: TreeWithSize<INodeDescriptor>): SumAndIncrease {
        // console.log("visitCols", colPath, rowIdx);

        if (isLeaf(col.node)) {
          const value = getValue(rowPath, colPath);
          outputRows[rowIdx][colIdx] = value;
          return {
            sum: value,
            increase: 1,
          };
        }
        let innerSum = 0;
        let innerIncrease = 1;
        if (col.children) {
          col.children.forEach((colChild) => {
            const { sum: childSum, increase: childIncrease } = visitCols(
              [...colPath, colChild.node.name], colIdx + innerIncrease, colChild,
            );
            innerSum += childSum;
            innerIncrease += childIncrease;
          });
          // console.log("sum", rowPath, rowIdx, colPath, colIdx, innerSum);
          outputRows[rowIdx][colIdx] = innerSum;
        }
        return {
          sum: innerSum,
          increase: innerIncrease,
        };
      }

      let increase = 1;
      let sum = 0;
      if (isLeaf(row.node)) {
        const { sum: colsSum } = visitCols([], 0, cols);
        sum += colsSum;
      } else {
        row.children.forEach((rowChild) => {
          const { sum: childSum, increase: childIncrease } = visitRows(
            [...rowPath, rowChild.node.name], rowIdx + increase, rowChild,
          );
          for (let c = 0; c < cols.size; c++) {
            outputRows[rowIdx][c] += outputRows[rowIdx + increase][c];
          }
          sum += childSum;
          increase += childIncrease;
        });
      }

      return {
        sum,
        increase,
      };
    }

    visitRows([], 0, rows);
    return outputRows;
  },
);

const selectFlattenedRows = createSelector(
  selectMasterGrid,
  selectRowsWithSize,
  (propsAndState: PropsAndState) => propsAndState.state.collapsedRows,
  (propsAndState: PropsAndState) => propsAndState.state.paginationStates,
  (
    masterGrid: number[][],
    rows: TreeWithSize<SchemaObject>,
    collapsedRows: TreePath[],
    paginationStates: AssocList<TreePath, PaginationState>,
  ) => {
    console.log("flattening rows");
    const sortBy = (masterRowIdx: number) => {
      return masterGrid[masterRowIdx][0];
    };
    return flatten(rows, collapsedRows, true /* includeNodes */, paginationStates, PAGE_SIZE, sortBy);
  },
);

const selectFlattenedCols = createSelector(
  selectColsWithSize,
  (propsAndState: PropsAndState) => propsAndState.state.collapsedCols,
  (cols: TreeWithSize<INodeDescriptor>, collapseCols: TreePath[]) => {
    console.log("flattening cols");
    return flatten(cols, collapseCols, false /* includeNodes */);
  },
);

const selectAllVals = createSelector(
  selectMasterGrid,
  selectFlattenedRows,
  selectFlattenedCols,
  (
    masterGrid: number[][],
    flattenedRows: FlattenedNode<SchemaObject>[],
    flattenedCols: FlattenedNode<INodeDescriptor>[],
  ) => {
    const allVals: number[][] = [];
    const inSingleArray: number[] = [];
    flattenedRows.forEach((row) => {
      const rowVals: number[] = [];
      allVals.push(rowVals);
      flattenedCols.forEach((col) => {
        if (!(row.isLeaf || row.isCollapsed)) {
          return;
        }
        const value = masterGrid[row.masterIdx][col.masterIdx];
        rowVals.push(value);
        inSingleArray.push(value);
      });
    });
    return {
      allVals,
      inSingleArray,
    };
  },
);

const selectScale = createSelector(
  selectAllVals,
  ({ inSingleArray }) => {
    console.log("computing scale");
    const extent = d3.extent(inSingleArray);
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
