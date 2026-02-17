// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { useSelector } from "react-redux";
import { Link, useLocation } from "react-router-dom";

import { SideNavigation } from "src/components";
import "./navigation-bar.scss";
import { isSingleNodeCluster as isSingleNodeClusterSelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";

interface RouteParam {
  path: string;
  text: string;
  activeFor: string[];
  // ignoreFor allows exclude specific paths from been recognized as active
  // even if path is matched with activeFor paths.
  ignoreFor?: string[];
  isHidden?: boolean;
}

/**
 * Sidebar represents the static navigation sidebar available on all pages. It
 * displays a number of graphic icons representing available pages; the icon of
 * the page which is currently active will be highlighted.
 */
function Sidebar(): React.ReactElement {
  const singleNode = useSelector((state: AdminUIState) =>
    isSingleNodeClusterSelector(state),
  );
  const location = useLocation();

  const routes: RouteParam[] = [
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
      isHidden: singleNode,
    },
    {
      path: "/topranges",
      text: "Top Ranges",
      activeFor: ["/hotranges", "/hotranges", "/reports/range"],
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

  const isActiveNavigationItem = (path: string): boolean => {
    const { activeFor, ignoreFor = [] } = routes.find(
      route => route.path === path,
    );
    return [...activeFor, path].some(p => {
      const isMatchedToIgnoredPaths = ignoreFor.some(ignorePath =>
        location.pathname.startsWith(ignorePath),
      );
      const isMatchedToActiveFor = location.pathname.startsWith(p);
      return isMatchedToActiveFor && !isMatchedToIgnoredPaths;
    });
  };

  const navigationItems = routes
    .filter(route => !route.isHidden)
    .map(({ path, text }, idx) => (
      <SideNavigation.Item isActive={isActiveNavigationItem(path)} key={idx}>
        <Link to={path}>{text}</Link>
      </SideNavigation.Item>
    ));
  return <SideNavigation>{navigationItems}</SideNavigation>;
}

export default Sidebar;
