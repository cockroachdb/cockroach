import * as React from "react";
import _ = require("lodash");

/**
 * SortableColumn describes the contents a single column of a
 * sortable table.
 */
export interface SortableColumn {
  // Text that will appear in the title header of the table.
  title: React.ReactNode;
  // Function which provides the contents for this column for a given row index
  // in the dataset.
  cell: (rowIndex: number) => React.ReactNode;
  // Contents that will appear in the "rollup" header of this column.
  rollup?: React.ReactNode;
  // Unique key that identifies this column from others, for the purpose of
  // indicating sort order. If not provided, the column is not considered
  // sortable.
  sortKey?: any;
  // className is a classname to apply to the td elements
  className?: string;
}

/**
 * SortSetting is the structure that SortableTable uses to indicate its current
 * sort preference to higher-level components. It contains a sortKey (taken from
 * one of the currently displayed columns) and a boolean indicating that the
 * sort should be ascending, rather than descending.
 */
export interface SortSetting {
  sortKey: any;
  ascending: boolean;
}

/**
 * TableProps are the props that should be passed to SortableTable.
 */
interface TableProps {
  // Number of rows in the table.
  count: number;
  // Current sortSetting that was used to sort incoming data.
  sortSetting?: SortSetting;
  // Callback that should be invoked when the user want to change the sort
  // setting.
  onChangeSortSetting?: { (ss: SortSetting): void };
}

/**
 * SortableTable is designed to display tabular data where the data set can be
 * sorted by one or more columns.
 *
 * SortableTable is not responsible for sorting data; however, it does allow the
 * user to indicate how data should be sorted by clicking on column headers.
 * SortableTable can indicate this to higher-level components through the
 * 'onChangeSortSetting' callback property.
 *
 * SortableColumns should be passed to SortableTable as children.
 */
export class SortableTable extends React.Component<TableProps, {}> {
  static defaultProps: TableProps = {
      count: 0,
      sortSetting: {
        sortKey: null,
        ascending: false,
      },
      onChangeSortSetting: (ss) => {},
  };

  clickSort(clickedSortKey: any) {
    let { sortSetting, onChangeSortSetting } = this.props;

    // If the sort key is different than the previous key, initial sort
    // descending. If the same sort key is clicked multiple times consecutively,
    // first change to ascending, then remove the sort key.
    let ascending = false;
    if (sortSetting.sortKey === clickedSortKey) {
      if (!sortSetting.ascending) {
        ascending = true;
      } else {
        clickedSortKey = null;
      }
    }

    onChangeSortSetting({
      sortKey: clickedSortKey,
      ascending,
    });
  }

  render() {
    let columns = this.props.children as SortableColumn[];
    let { sortSetting } = this.props;

    return <table>
     <thead>
        <tr className="column">
          {_.map(columns, (c: SortableColumn, colIndex: number) => {
            let className = "column";
            let onClick: (e: any) => void = undefined;

            if (c.sortKey) {
              onClick = () => {
                this.clickSort(c.sortKey);
              };
              if (c.sortKey === sortSetting.sortKey) {
                className += " sorted";
                if (sortSetting.ascending) {
                  className += " ascending";
                }
              }
            }
            return <th className={className} key={colIndex} onClick={onClick}>{c.title}</th>;
          })}
        </tr>
        <tr className="rollup">
          {_.map(columns, (c: SortableColumn, colIndex: number) => {
            return <th className="rollup" key={colIndex}>{c.rollup}</th>;
          })}
        </tr>
      </thead>
      <tbody>
        {_.times(this.props.count, (rowIndex) => {
          return <tr key={rowIndex}>
            {_.map(columns, (c: SortableColumn, colIndex: number) => <td className={c.className} key={colIndex}>{c.cell(rowIndex)}</td>)}
            </tr>;
        })}
      </tbody>
    </table>;
  }
}
