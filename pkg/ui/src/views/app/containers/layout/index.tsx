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

import NavigationBar from "src/views/app/components/layoutSidebar";
import TimeWindowManager from "src/views/app/containers/timewindow";
import AlertBanner from "src/views/app/containers/alertBanner";
import RequireLogin from "src/views/login/requireLogin";
import { GlobalNavigation, CockroachLabsLockupIcon, Left, Right, TabNavigation, Text, TextTypes } from "src/components";
import LoginIndicator from "src/views/app/components/loginIndicator";

import "./layout.styl";
import "./layoutPanel.styl";

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
        <Helmet
          titleTemplate="%s | Cockroach Console"
          defaultTitle="Cockroach Console"
        />
        <TimeWindowManager/>
        <AlertBanner/>
        <div className="layout-panel">
          <div className="layout-panel__header">
            <GlobalNavigation>
              <Left>
                <CockroachLabsLockupIcon height={26} />
              </Left>
              <Right>
                <LoginIndicator />
              </Right>
            </GlobalNavigation>
          </div>
          <div className="layout-panel__navigation-bar">
            <TabNavigation>
              <Text textType={TextTypes.Heading2}>Cluster - 01</Text>
              <Text
                textType={TextTypes.Body}
                disabled
              >
                Cockroach version 19.2 / 12 regions / 54 machines
              </Text>
            </TabNavigation>
          </div>
          <div className="layout-panel__body">
            <div className="layout-panel__sidebar">
              <NavigationBar/>
            </div>
            <div className="layout-panel__content">
              { this.props.children }
            </div>
          </div>
        </div>
      </RequireLogin>
    );
  }
}
