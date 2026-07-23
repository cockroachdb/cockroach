// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretLeftOutlined, CaretDownOutlined } from "@ant-design/icons";
import cn from "classnames";
import React, { useEffect, useRef, useState } from "react";
import { connect } from "react-redux";
import { Action, Dispatch } from "redux";

import { Text, TextTypes } from "src/components";
import { LocalSetting, setLocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";

import "./tableSection.scss";

interface MapStateToProps {
  isCollapsed: boolean;
}

interface MapDispatchToProps {
  saveExpandedState: (isCollapsed: boolean) => void;
}

interface TableSectionProps {
  // Uniq ID which is used to create LocalSettings key for storing user state in session.
  id: string;
  // Title of the section
  title: string;
  // Bottom part of the section, can be string or RectNode.
  footer?: React.ReactNode;
  // If true, allows collapse content of the section and only title remains visible.
  isCollapsible?: boolean;
  // Class which is applied on root element of the section.
  className?: string;
  children?: React.ReactNode;
}

function TableSection({
  title,
  footer,
  isCollapsible = false,
  className = "",
  children,
  saveExpandedState,
}: TableSectionProps &
  MapStateToProps &
  MapDispatchToProps): React.ReactElement {
  const [isCollapsed, setIsCollapsed] = useState(false);

  // Keep a ref to the latest isCollapsed value so the unmount cleanup
  // doesn't capture a stale value from the initial render.
  const isCollapsedRef = useRef(isCollapsed);
  isCollapsedRef.current = isCollapsed;

  useEffect(() => {
    return () => {
      saveExpandedState(isCollapsedRef.current);
    };
  }, [saveExpandedState]);

  const onExpandSectionToggle = () => {
    const newValue = !isCollapsed;
    setIsCollapsed(newValue);
    saveExpandedState(newValue);
  };

  const rootClass = cn("table-section", className);
  const contentClass = cn("table-section__content", {
    "table-section__content--collapsed": isCollapsed,
  });

  return (
    <div className={rootClass}>
      <section className="table-section__heading table-section__heading--justify-end">
        <Text textType={TextTypes.Heading3}>{title}</Text>
        {isCollapsible && (
          <div className="collapse-toggle" onClick={onExpandSectionToggle}>
            <span>{isCollapsed ? "Show" : "Hide"}</span>
            {isCollapsed ? (
              <CaretLeftOutlined className="collapse-toggle__icon" />
            ) : (
              <CaretDownOutlined className="collapse-toggle__icon" />
            )}
          </div>
        )}
      </section>
      <div className={contentClass}>
        {children}
        {footer && <div className="table-section__footer">{footer}</div>}
      </div>
    </div>
  );
}

const getTableSectionKey = (id: string) =>
  `cluster_overview/table_section/${id}/is_expanded`;

const mapStateToProps = (
  state: AdminUIState,
  props: TableSectionProps,
): MapStateToProps => {
  const tableSectionState = new LocalSetting<AdminUIState, boolean>(
    getTableSectionKey(props.id),
    s => s.localSettings,
  );

  return {
    isCollapsed: tableSectionState.selector(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch<Action>,
  props: TableSectionProps,
): MapDispatchToProps => ({
  saveExpandedState: (isCollapsed: boolean) => {
    const tableSectionKey = getTableSectionKey(props.id);
    dispatch(setLocalSetting(tableSectionKey, isCollapsed));
  },
});

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  TableSectionProps,
  AdminUIState
>(
  mapStateToProps,
  mapDispatchToProps,
)(TableSection);
