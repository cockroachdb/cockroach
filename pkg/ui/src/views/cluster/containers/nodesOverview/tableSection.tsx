// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import { connect } from "react-redux";
import cn from "classnames";
import { Icon } from "antd";
import { Action, Dispatch } from "redux";

import { LocalSetting, setLocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { Text, TextTypes } from "src/components";

import "./tableSection.styl";

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

interface TableSectionState {
  isCollapsed: boolean;
}

class TableSection extends React.Component<
  TableSectionProps & MapStateToProps & MapDispatchToProps,
  TableSectionState
> {
  static defaultProps: Partial<TableSectionProps> = {
    isCollapsible: false,
    footer: null,
    className: "",
  };

  state = {
    isCollapsed: false,
  };

  componentWillUnmount() {
    this.props.saveExpandedState(this.state.isCollapsed);
  }

  onExpandSectionToggle = () => {
    this.setState({
      isCollapsed: !this.state.isCollapsed,
    });
    this.props.saveExpandedState(!this.state.isCollapsed);
  };

  getCollapseSectionToggle = () => {
    const { isCollapsible } = this.props;
    if (!isCollapsible) {
      return null;
    }

    const { isCollapsed } = this.state;

    return (
      <div className="collapse-toggle" onClick={this.onExpandSectionToggle}>
        <span>{isCollapsed ? "Show" : "Hide"}</span>
        <Icon
          className="collapse-toggle__icon"
          type={isCollapsed ? "caret-left" : "caret-down"}
        />
      </div>
    );
  };

  render() {
    const { children, title, footer, className } = this.props;
    const { isCollapsed } = this.state;
    const rootClass = cn("table-section", className);
    const contentClass = cn("table-section__content", {
      "table-section__content--collapsed": isCollapsed,
    });
    const collapseToggleButton = this.getCollapseSectionToggle();

    return (
      <div className={rootClass}>
        <section className="table-section__heading table-section__heading--justify-end">
          <Text textType={TextTypes.Heading3}>{title}</Text>
          {collapseToggleButton}
        </section>
        <div className={contentClass}>
          {children}
          {footer && <div className="table-section__footer">{footer}</div>}
        </div>
      </div>
    );
  }
}

const getTableSectionKey = (id: string) =>
  `cluster_overview/table_section/${id}/is_expanded`;

const mapStateToProps = (state: AdminUIState, props: TableSectionProps) => {
  const tableSectionState = new LocalSetting<AdminUIState, boolean>(
    getTableSectionKey(props.id),
    (s) => s.localSettings,
  );

  return {
    isCollapsed: tableSectionState.selector(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch<Action, AdminUIState>,
  props: TableSectionProps,
) => ({
  saveExpandedState: (isCollapsed: boolean) => {
    const tableSectionKey = getTableSectionKey(props.id);
    dispatch(setLocalSetting(tableSectionKey, isCollapsed));
  },
});

export default connect(mapStateToProps, mapDispatchToProps)(TableSection);
