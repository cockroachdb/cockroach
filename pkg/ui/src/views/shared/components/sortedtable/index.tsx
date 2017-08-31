import React from "react";
import _ from "lodash";
import { createSelector } from "reselect";

import { SortableTable, SortableColumn, SortSetting } from "src/views/shared/components/sortabletable";

/**
 * ColumnDescriptor is used to describe metadata about an individual column
 * in a SortedTable.
 */
export interface ColumnDescriptor<T> {
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (obj: T) => React.ReactNode;
  // Function which returns a value that can be used to sort the collection of
  // objects. This will be used to sort the table according to the data in
  // this column.
  sort?: (obj: T) => any;
  // Function that generates a "rollup" value for this column from all objects
  // in a collection. This is used to display an appropriate "total" value for
  // each column.
  rollup?: (objs: T[]) => React.ReactNode;
  // className to be applied to the td elements in this column.
  className?: string;
}

/**
 * SortedTableProps describes the properties expected by a SortedTable
 * component.
 */
interface SortedTableProps<T> {
  // The data which should be displayed in the table. This data may be sorted
  // by this component before display.
  data: T[];
  // Description of columns to display.
  columns: ColumnDescriptor<T>[];
  // sortSetting specifies how data should be sorted in this table, by
  // specifying a column id and a direction.
  sortSetting: SortSetting;
  // Callback that should be invoked when the user want to change the sort
  // setting.
  onChangeSortSetting?: { (ss: SortSetting): void };
  // className to be applied to the table element.
  className?: string;
  // A function that returns the class to apply to a given row.
  rowClass?: (obj: T) => string;
}

/**
 * SortedTable displays data rows in a table which can be sorted by the values
 * in a single column. Unsorted row data is passed to SortedTable along with
 * a SortSetting; SortedTable uses a selector to sort the data for display.
 *
 * SortedTable also computes optional "rollup" values for each column; a rollup
 * is a total value that is computed for a column based on all available rows.
 *
 * SortedTable should be preferred over the lower-level SortableTable when
 * all data rows to be displayed are available locally on the client side.
 */
export class SortedTable<T> extends React.Component<SortedTableProps<T>, {}> {
  static defaultProps: Partial<SortedTableProps<any>> = {
    rowClass: (_obj: any) => "",
  };

  rollups = createSelector(
    (props: SortedTableProps<T>) => props.data,
    (props: SortedTableProps<T>) => props.columns,
    (data: T[], columns: ColumnDescriptor<T>[]) => {
      return _.map(columns, (c): React.ReactNode => {
        if (c.rollup) {
          return c.rollup(data);
        }
        return undefined;
      });
    },
  );

  sorted = createSelector(
    (props: SortedTableProps<T>) => props.data,
    (props: SortedTableProps<T>) => props.sortSetting,
    (props: SortedTableProps<T>) => props.columns,
    (data: T[], sortSetting: SortSetting, columns: ColumnDescriptor<T>[]) => {
      if (!sortSetting) {
        return data;
      }
      const sortColumn = columns[sortSetting.sortKey];
      if (!sortColumn || !sortColumn.sort) {
        return data;
      }
      return _.orderBy(data, sortColumn.sort, sortSetting.ascending ? "asc" : "desc");
    },
  );

  /**
   * columns is a selector which computes the input columns to the underlying
   * sortableTable.
   */
  columns = createSelector(
    this.sorted,
    this.rollups,
    (props: SortedTableProps<T>) => props.columns,
    (sorted: T[], rollups: React.ReactNode[], columns: ColumnDescriptor<T>[]) => {
      return _.map(columns, (cd, ii): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(sorted[index]),
          sortKey: cd.sort ? ii : undefined,
          rollup: rollups[ii],
          className: cd.className,
        };
      });
    });

  rowClass = createSelector(
    this.sorted,
    (props: SortedTableProps<T>) => props.rowClass,
    (sorted: T[], rowClass: (obj: T) => string) => {
      return (index: number) => rowClass(sorted[index]);
    },
  );

  render() {
    const { data, sortSetting, onChangeSortSetting } = this.props;
    if (data) {
      return (
        <SortableTable count={data.length}
          sortSetting={sortSetting}
          onChangeSortSetting={onChangeSortSetting}
          columns={this.columns(this.props)}
          rowClass={this.rowClass(this.props)}
          className={this.props.className}
        />
      );
    }
    return <div>No results.</div>;
  }
}
