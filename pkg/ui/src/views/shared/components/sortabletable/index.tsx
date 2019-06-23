// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import _ from "lodash";
import classNames from "classnames";

import "./sortabletable.styl";

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
  // Array of SortableColumns to render.
  columns: SortableColumn[];
  // Current sortSetting that was used to sort incoming data.
  sortSetting?: SortSetting;
  // Callback that should be invoked when the user want to change the sort
  // setting.
  onChangeSortSetting?: { (ss: SortSetting): void };
  // className to be applied to the table element.
  className?: string;
  // A function that returns the class to apply to a given row.
  rowClass?: (rowIndex: number) => string;
  // expandableConfig, if provided, makes each row in the table "expandable",
  // i.e. each row has an expand/collapse arrow on its left, and renders
  // a full-width area below it when expanded.
  expandableConfig?: ExpandableConfig;
}

export interface ExpandableConfig {
  // Called when the expand toggle is clicked. If this prop is not supplied,
  // the table is not expandable and the expansion control is not shown.
  onChangeExpansion: (rowIndex: number, expanded: boolean) => void;
  // Given a row index, return whether that row is expanded.
  rowIsExpanded: (rowIndex: number) => boolean;
  // If a row is expanded, this function is called to get the content for the
  // full-width expanded section.
  expandedContent: (rowIndex: number) => React.ReactNode;
}

/**
 * SortableTable is designed to display tabular data where the data set can be
 * sorted by one or more columns.
 *
 * SortableTable is not responsible for sorting data; however, it does allow the
 * user to indicate how data should be sorted by clicking on column headers.
 * SortableTable can indicate this to higher-level components through the
 * 'onChangeSortSetting' callback property.
 */
export class SortableTable extends React.Component<TableProps> {
  static defaultProps: TableProps = {
    count: 0,
    columns: [],
    sortSetting: {
      sortKey: null,
      ascending: false,
    },
    onChangeSortSetting: (_ss) => { },
    rowClass: (_rowIndex) => "",
  };

  clickSort(clickedSortKey: any) {
    const { sortSetting, onChangeSortSetting } = this.props;

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

  expansionControl(expanded: boolean) {
    const content = expanded ? "▼" : "▶";
    return (
      <td className="sort-table__cell sort-table__cell__expansion-control">
        <div>
          {content}
        </div>
      </td>
    );
  }

  renderRow = (rowIndex: number) => {
    const { columns, expandableConfig } = this.props;

    const classes = classNames(
      "sort-table__row",
      "sort-table__row--body",
      this.props.rowClass(rowIndex),
      { "sort-table__row--expandable": !!expandableConfig },
    );
    const expanded = expandableConfig && expandableConfig.rowIsExpanded(rowIndex);
    const onClickExpand = expandableConfig && expandableConfig.onChangeExpansion;
    const output = [
      <tr
        key={rowIndex}
        className={classes}
        onClick={() => onClickExpand(rowIndex, !expanded)}
      >
        {expandableConfig ? this.expansionControl(expanded) : null}
        {_.map(columns, (c: SortableColumn, colIndex: number) => {
          return (
            <td className={classNames("sort-table__cell", c.className)} key={colIndex}>
              {c.cell(rowIndex)}
            </td>
          );
        })
        }
      </tr>,
    ];
    if (expandableConfig && expandableConfig.rowIsExpanded(rowIndex)) {
      const expandedAreaClasses = classNames(
        "sort-table__row",
        "sort-table__row--body",
        "sort-table__row--expanded-area",
      );
      output.push(
        // Add a zero-height empty row so that the expanded area will have the same background
        // color as the row it's expanded from, since the CSS causes row colors to alternate.
        <tr className={classes} />,
        <tr className={expandedAreaClasses}>
          <td />
          <td
            className="sort-table__cell"
            colSpan={columns.length}
          >
            {expandableConfig.expandedContent(rowIndex)}
          </td>
        </tr>,
      );
    }
    return output;
  }

  render() {
    const { sortSetting, columns, expandableConfig } = this.props;
    return (
      <table className={classNames("sort-table", this.props.className)}>
        <thead>
          <tr className="sort-table__row sort-table__row--header">
            {expandableConfig ? <th className="sort-table__cell" /> : null}
            {_.map(columns, (c: SortableColumn, colIndex: number) => {
              const classes = ["sort-table__cell"];
              let onClick: (e: any) => void = undefined;

              if (!_.isUndefined(c.sortKey)) {
                classes.push("sort-table__cell--sortable");
                onClick = () => {
                  this.clickSort(c.sortKey);
                };
                if (c.sortKey === sortSetting.sortKey) {
                  if (sortSetting.ascending) {
                    classes.push(" sort-table__cell--ascending");
                  } else {
                    classes.push("sort-table__cell--descending");
                  }
                }
              }
              return (
                <th className={classNames(classes)} key={colIndex} onClick={onClick}>
                  {c.title}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {_.times(this.props.count, this.renderRow)}
        </tbody>
      </table>
    );
  }
}
