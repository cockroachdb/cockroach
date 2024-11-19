// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { connect } from "react-redux";
import { Link, withRouter, RouteComponentProps } from "react-router-dom";

import { SideNavigation } from "src/components";
import "./navigation-bar.styl";
import { isSingleNodeCluster } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";

interface RouteParam {
  path: string;
  text: string;
  activeFor: string[];
  // ignoreFor allows exclude specific paths from been recognized as active
  // even if path is matched with activeFor paths.
  ignoreFor?: string[];
  isHidden?: () => boolean;
}

type SidebarProps = RouteComponentProps & MapToStateProps;

/**
 * Sidebar represents the static navigation sidebar available on all pages. It
 * displays a number of graphic icons representing available pages; the icon of
 * the page which is currently active will be highlighted.
 */
export class Sidebar extends React.Component<SidebarProps> {
  readonly routes: RouteParam[] = [
    { path: "/overview", text: "Overview", activeFor: ["/node"] },
    { path: "/metrics", text: "Metrics", activeFor: [] },
    { path: "/databases", text: "Databases", activeFor: ["/database"] },
    {
      path: "/sql-activity",
      text: "SQL Activity",
      activeFor: ["/sql-activity", "/session", "/transaction", "/statement"],
    },
    {
      path: "/insights",
      text: "Insights",
      activeFor: ["/insights"],
    },
    {
      path: "/reports/network",
      text: "Network",
      activeFor: ["/reports/network"],
      // Do not show Network for single node cluster.
      isHidden: () => this.props.isSingleNodeCluster,
    },
    {
      path: "/hotranges",
      text: "Hot Ranges",
      activeFor: ["/hotranges", "/reports/range"],
    },
    { path: "/jobs", text: "Jobs", activeFor: [] },
    { path: "/schedules", text: "Schedules", activeFor: [] },
    {
      path: "/debug",
      text: "Advanced Debug",
      activeFor: ["/reports", "/data-distribution", "/raft", "/keyvisualizer"],
      ignoreFor: ["/reports/network", "/reports/range"],
    },
  ];

  isActiveNavigationItem = (path: string): boolean => {
    const { pathname } = this.props.location;
    const { activeFor, ignoreFor = [] } = this.routes.find(
      route => route.path === path,
    );
    return [...activeFor, path].some(p => {
      const isMatchedToIgnoredPaths = ignoreFor.some(ignorePath =>
        pathname.startsWith(ignorePath),
      );
      const isMatchedToActiveFor = pathname.startsWith(p);
      return isMatchedToActiveFor && !isMatchedToIgnoredPaths;
    });
  };

  render() {
    const navigationItems = this.routes
      .filter(route => !route.isHidden || !route.isHidden())
      .map(({ path, text }, idx) => (
        <SideNavigation.Item
          isActive={this.isActiveNavigationItem(path)}
          key={idx}
        >
          <Link to={path}>{text}</Link>
        </SideNavigation.Item>
      ));
    return <SideNavigation>{navigationItems}</SideNavigation>;
  }
}

interface MapToStateProps {
  isSingleNodeCluster: boolean;
}

const mapStateToProps = (state: AdminUIState) => ({
  isSingleNodeCluster: isSingleNodeCluster(state),
});

export default withRouter(connect(mapStateToProps, null)(Sidebar));
