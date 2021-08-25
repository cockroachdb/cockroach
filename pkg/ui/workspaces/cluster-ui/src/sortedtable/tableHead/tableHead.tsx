// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import styles from "./tableHead.module.scss";
import { ExpandableConfig, SortableColumn, SortSetting } from "../sortedtable";

const cx = classNames.bind(styles);

interface TableHeadProps {
  columns: SortableColumn[];
  expandableConfig?: ExpandableConfig;
  onChangeSortSetting?: { (ss: SortSetting): void };
  sortSetting?: SortSetting;
  firstCellBordered: boolean;
}

export const TableHead: React.FC<TableHeadProps> = ({
  expandableConfig,
  columns,
  sortSetting,
  onChangeSortSetting,
  firstCellBordered,
}) => {
  const trClass = cx("head-wrapper__row", "head-wrapper__row--header");
  const thClass = cx("head-wrapper__cell");
  const cellContentWrapper = cx("inner-content-wrapper");
  const arrowsClass = cx("sortable__actions");

  function handleSort(picked: boolean, columnTitle: string) {
    // If the columnTitle is different than the previous value, initial sort
    // descending. If the same columnTitle is clicked multiple times consecutively,
    // first change to ascending, then remove the sort key.
    const ASCENDING = true;
    const DESCENDING = false;

    const direction = picked ? ASCENDING : DESCENDING;
    const sortElementColumnTitle =
      picked && sortSetting.ascending ? null : columnTitle;

    onChangeSortSetting({
      ascending: direction,
      columnTitle: sortElementColumnTitle,
    });
  }

  return (
    <thead>
      <tr className={trClass}>
        {expandableConfig && <th className={thClass} />}
        {columns.map((c: SortableColumn, idx: number) => {
          const sortable = c.columnTitle !== (null || undefined);
          const picked = c.name === sortSetting.columnTitle;
          const style = { textAlign: c.titleAlign };
          const cellAction = sortable ? () => handleSort(picked, c.name) : null;
          const cellClasses = cx(
            "head-wrapper__cell",
            "sorted__cell",
            sortable && "sorted__cell--sortable",
            sortSetting.ascending && picked && "sorted__cell--ascending",
            !sortSetting.ascending && picked && "sorted__cell--descending",
            firstCellBordered && idx === 0 && "cell-header",
          );
          const titleClasses = cx("column-title");

          return (
            <th
              className={classNames(cellClasses)}
              key={"headCell" + idx}
              onClick={cellAction}
              style={style}
            >
              <div className={cellContentWrapper}>
                <span className={titleClasses}>{c.title} </span>
                {sortable && <span className={arrowsClass} />}
              </div>
            </th>
          );
        })}
      </tr>
    </thead>
  );
};

TableHead.defaultProps = {
  onChangeSortSetting: _ss => {},
};
