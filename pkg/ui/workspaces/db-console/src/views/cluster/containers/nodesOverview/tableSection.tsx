// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretLeftOutlined, CaretDownOutlined } from "@ant-design/icons";
import cn from "classnames";
import * as React from "react";
import { connect } from "react-redux";
import { Action, Dispatch } from "redux";

import { Text, TextTypes } from "src/components";
import { LocalSetting, setLocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";

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
  static defaultProps = {
    isCollapsible: false,
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
        {isCollapsed ? (
          <CaretLeftOutlined className="collapse-toggle__icon" />
        ) : (
          <CaretDownOutlined className="collapse-toggle__icon" />
        )}
      </div>
    );
  };

  render() {
    const { children, title, className } = this.props;
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
          {this.props.footer && (
            <div className="table-section__footer">{this.props.footer}</div>
          )}
        </div>
      </div>
    );
  }
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
