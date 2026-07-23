// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Icon } from "@cockroachlabs/ui-components";
import classnames from "classnames/bind";
import React from "react";

import Breadcrumbs from "./breadcrumbs";
import DelayedLoaderIcon from "./delayedLoaderIcon";
import styles from "./pageHeader.module.scss";
import { Skeleton } from "./skeleton";

import type { BreadcrumbItemWithLink } from "./breadcrumbs";
import type { SystemIcons } from "@cockroachlabs/icons";

const cx = classnames.bind(styles);

export interface PageHeader {
  breadcrumbItems?: BreadcrumbItemWithLink[];
  iconName?: keyof typeof SystemIcons;
  title?: string | React.ReactElement;
  description?: string | React.ReactElement;
  actions?: React.ReactElement[] | React.ReactElement;
  reduceMargin?: boolean; // Some pieces of content below a header can already have vertical padding.
  className?: string;
  isValidating?: boolean;
}

/**
 * PageHeader provides a standardized header that can be used across the main and cluster details pages.
 * @param breadcrumbItems - Breadcrumb trail that is defined at the top of the header.
 * @param icon - Optional icon to the left of the title.
 * @param title - Title of the page.
 * @param description - Text or component description rendered underneath the title.
 * @param actions - Generally a list of buttons or inputs that can be rendered to the right of a header.
 * @param reduceMargin - Used to remove some padding that implicitly exists when using without a button,
 *                       above an element like horizontal tabs.
 * @param className - Custom classname for styling the component.
 * @returns A friendly component for standardizing headers :)
 */
export const PageHeader = ({
  breadcrumbItems,
  iconName,
  title,
  description,
  actions,
  reduceMargin,
  className,
  isValidating,
}: PageHeader) => {
  // Renders a <p> tag for string descriptions, or the element passed into the component for other types.
  const renderDescription = () => {
    if (!description) return null;
    return typeof description === "string" ? (
      <p className={cx("description")}>{description}</p>
    ) : (
      description
    );
  };

  return (
    <div
      className={cx("page-header", className, {
        "page-header--reduced-margin": reduceMargin,
      })}
    >
      {breadcrumbItems && (
        <Breadcrumbs
          divider={<Icon iconName="CaretRight" size="tiny" />}
          items={breadcrumbItems}
          className={cx("header-breadcrumbs")}
        />
      )}
      <div className={cx("header-content")}>
        <div>
          <h3 className={cx("title")}>
            {iconName && (
              <Icon className={cx("icon")} iconName={iconName} size="large" />
            )}
            {title || <Skeleton />}

            {isValidating && (
              <DelayedLoaderIcon size="small" className={cx("loader")} />
            )}
          </h3>
          {renderDescription()}
        </div>
        <div className={cx("action-wrapper")}>{actions ? actions : null}</div>
      </div>
    </div>
  );
};
