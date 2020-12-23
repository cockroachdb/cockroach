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
import { RouteComponentProps, withRouter } from "react-router-dom";
import { connect } from "react-redux";

import NavigationBar from "src/views/app/components/layoutSidebar";
import TimeWindowManager from "src/views/app/containers/timewindow";
import AlertBanner from "src/views/app/containers/alertBanner";
import RequireLogin from "src/views/login/requireLogin";
import {
  clusterIdSelector,
  clusterNameSelector,
  singleVersionSelector,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import LoginIndicator from "src/views/app/components/loginIndicator";
import {
  GlobalNavigation,
  CockroachLabsLockupIcon,
  Left,
  Right,
  PageHeader,
  Text,
  TextTypes,
  Badge,
} from "src/components";

import "./layout.styl";
import "./layoutPanel.styl";

export interface LayoutProps {
  clusterName: string;
  clusterVersion: string;
  clusterId: string;
}

/**
 * Defines the main layout of all admin ui pages. This includes static
 * navigation bars and footers which should be present on every page.
 *
 * Individual pages provide their content via react-router.
 */
class Layout extends React.Component<LayoutProps & RouteComponentProps> {
  contentRef = React.createRef<HTMLDivElement>();

  componentDidUpdate(prevProps: RouteComponentProps) {
    // `react-router` doesn't handle scroll restoration (https://reactrouter.com/react-router/web/guides/scroll-restoration)
    // and when location changed with react-router's Link it preserves scroll position whenever it is.
    // AdminUI layout keeps left and top panels have fixed position on a screen and has internal scrolling for content div
    // element which has to be scrolled back on top with navigation change.
    if (this.props.location.pathname !== prevProps.location.pathname) {
      this.contentRef.current.scrollTo(0, 0);
    }
  }

  render() {
    const { clusterName, clusterVersion, clusterId } = this.props;
    return (
      <RequireLogin>
        <Helmet
          titleTemplate="%s | Cockroach Console"
          defaultTitle="Cockroach Console"
        />
        <TimeWindowManager />
        <AlertBanner />
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
            <PageHeader>
              <Text textType={TextTypes.Heading2} noWrap>
                {clusterName || `Cluster id: ${clusterId || ""}`}
              </Text>
              <Badge text={clusterVersion} />
            </PageHeader>
          </div>
          <div className="layout-panel__body">
            <div className="layout-panel__sidebar">
              <NavigationBar />
            </div>
            <div ref={this.contentRef} className="layout-panel__content">
              {this.props.children}
            </div>
          </div>
        </div>
      </RequireLogin>
    );
  }
}

const mapStateToProps = (state: AdminUIState) => {
  return {
    clusterName: clusterNameSelector(state),
    clusterVersion: singleVersionSelector(state),
    clusterId: clusterIdSelector(state),
  };
};

export default withRouter(connect(mapStateToProps)(Layout));
