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
import { Helmet } from "react-helmet";
import { RouterState } from "react-router";
import { connect } from "react-redux";

import { ClusterNameProps, clusterNameSelector } from "src/redux/cluster";
import NavigationBar from "src/views/app/components/layoutSidebar";
import TimeWindowManager from "src/views/app/containers/timewindow";
import AlertBanner from "src/views/app/containers/alertBanner";
import RequireLogin from "src/views/login/requireLogin";

import "./layout.styl";

function renderHelmetWithClusterNameProps(props: ClusterNameProps) {
  let defaultTitle = "Cockroach Console";
  if (props.shortClusterName) {
    defaultTitle = `${props.shortClusterName} | ${defaultTitle}`;
  }
  return (
    <Helmet titleTemplate={`%s | ${defaultTitle}`} defaultTitle={defaultTitle} />
  );
}

// tslint:disable-next-line:variable-name
const HelmetConnected = connect(clusterNameSelector)(renderHelmetWithClusterNameProps);

/**
 * Defines the main layout of all admin ui pages. This includes static
 * navigation bars and footers which should be present on every page.
 *
 * Individual pages provide their content via react-router.
 */
export default class extends React.Component<RouterState, {}> {
  render() {
    return (
      <RequireLogin>
        <HelmetConnected />
        <TimeWindowManager/>
        <AlertBanner/>
        <NavigationBar/>
        <div className="page">
          { this.props.children }
        </div>
      </RequireLogin>
    );
  }
}
