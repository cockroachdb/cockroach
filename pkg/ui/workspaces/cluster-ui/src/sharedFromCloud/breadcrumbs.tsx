// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classnames from "classnames/bind";
import React, { ReactElement, ReactChild } from "react";
import { Link } from "react-router-dom";

import styles from "./breadcrumbs.module.scss";
const cx = classnames.bind(styles);

export interface BreadcrumbItemWithLink {
  name: ReactChild;
  link: string;
  onClick?: () => void;
}

interface BreadcrumbItem {
  name: ReactChild;
  key: string;
}

/** Whether the item is of type `BreadcrumbItemWithLink`. */
function isBreadcrumbWithLink(
  item: BreadcrumbItemWithLink | BreadcrumbItem,
): item is BreadcrumbItemWithLink {
  return (item as BreadcrumbItemWithLink).link !== undefined;
}

interface BreadcrumbsProps {
  items: (BreadcrumbItem | BreadcrumbItemWithLink)[];
  divider?: ReactElement;
  className?: string;
  /** Whether to use compact or default spacing for breadcrumbs. */
  condensed?: boolean;
}

function Breadcrumbs({
  items,
  divider = <>/</>,
  className,
  condensed = false,
}: BreadcrumbsProps) {
  if (items.length === 0) {
    return null;
  }
  const lastItem = items.slice(-1)[0];
  const itemsWithoutLast = items.slice(0, -1);
  return (
    <div
      className={cx(
        "breadcrumbs",
        { "breadcrumbs--condensed": condensed },
        className,
      )}
      aria-label="breadcrumbs"
    >
      {itemsWithoutLast.map(item => {
        const includeLink = isBreadcrumbWithLink(item);
        return (
          <div
            className={cx("item-container")}
            key={includeLink ? item.link : item.key}
          >
            {includeLink ? (
              <Link
                className={cx("item", "item-link")}
                to={item.link}
                onClick={item.onClick}
              >
                {item.name}
              </Link>
            ) : (
              <span className={cx("item")}>{item.name}</span>
            )}
            <span className={cx("item-divider")} aria-hidden={true}>
              {divider}
            </span>
          </div>
        );
      })}
      <div className={cx("item-container", "item-last")} aria-current="page">
        {lastItem?.name}
      </div>
    </div>
  );
}

export default Breadcrumbs;
