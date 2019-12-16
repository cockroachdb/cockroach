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
import { connect } from "react-redux";
import { Link, withRouter, WithRouterProps } from "react-router";

import { SideNavigation } from "src/components";

import "./navigation-bar.styl";

/**
 * Sidebar represents the static navigation sidebar available on all pages. It
 * displays a number of graphic icons representing available pages; the icon of
 * the page which is currently active will be highlighted.
 */
class Sidebar extends React.Component<{} & WithRouterProps> {
  readonly routes = [
    { path: "/overview", text: "Overview", activeFor: ["/node"] },
    { path: "/metrics", text: "Metrics", activeFor: [] },
    { path: "/databases", text: "Databases", activeFor: ["/database"] },
    { path: "/statements", text: "Statements", activeFor: ["/statement"] },
    { path: "/jobs", text: "Jobs", activeFor: [] },
    { path: "/debug", text: "Advanced Debug", activeFor: ["/reports", "/data-distribution", "/raft"] },
  ];

  isActiveNavigationItem = (path: string): boolean => {
    const { activeFor } = this.routes.find(route => route.path === path);
    return [...activeFor, path].some(p => this.props.router.isActive(p));
  }

  render() {
    const navigationItems = this.routes.map(({ path, text }) => (
      <SideNavigation.Item
        isActive={this.isActiveNavigationItem(path)}>
        <Link to={path}>{text}</Link>
      </SideNavigation.Item>
    ));
    return (
      <SideNavigation>
        {navigationItems}
      </SideNavigation>
    );
  }
}

export default connect(null, null)(withRouter(Sidebar));
