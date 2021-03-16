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

  function handleSort(thSortKey: any, picked: boolean, columnTitle?: string) {
    // If the sort key is different than the previous key, initial sort
    // descending. If the same sort key is clicked multiple times consecutively,
    // first change to ascending, then remove the sort key.
    const ASCENDING = true;
    const DESCENDING = false;

    const direction = picked ? ASCENDING : DESCENDING;
    const sortElementKey = picked && sortSetting.ascending ? null : thSortKey;

    onChangeSortSetting({
      sortKey: sortElementKey,
      ascending: direction,
      columnTitle,
    });
  }

  return (
    <thead>
      <tr className={trClass}>
        {expandableConfig && <th className={thClass} />}
        {columns.map((c: SortableColumn, idx: number) => {
          const sortable = c.sortKey !== (null || undefined);
          const picked = c.sortKey === sortSetting.sortKey;
          const style = { textAlign: c.titleAlign };
          const cellAction = sortable
            ? () => handleSort(c.sortKey, picked, c.name)
            : null;
          const cellClasses = cx(
            "head-wrapper__cell",
            "sorted__cell",
            sortable && "sorted__cell--sortable",
            sortSetting.ascending && picked && "sorted__cell--ascending",
            !sortSetting.ascending && picked && "sorted__cell--descending",
            firstCellBordered && idx === 0 && "cell-header",
          );

          return (
            <th
              className={classNames(cellClasses)}
              key={"headCell" + idx}
              onClick={cellAction}
              style={style}
            >
              <div className={cellContentWrapper}>
                {c.title}
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
