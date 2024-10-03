// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Badge } from "@cockroachlabs/cluster-ui";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import {
  GlobalNavigation,
  CockroachLabsLockupIcon,
  Left,
  Right,
  PageHeader,
  Text,
  TextTypes,
} from "src/components";
import {
  clusterIdSelector,
  clusterNameSelector,
  clusterVersionLabelSelector,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { getDataFromServer } from "src/util/dataFromServer";
import ErrorBoundary from "src/views/app/components/errorMessage/errorBoundary";
import NavigationBar from "src/views/app/components/layoutSidebar";
import LoginIndicator from "src/views/app/components/loginIndicator";
import AlertBanner from "src/views/app/containers/alertBanner";
import TimeWindowManager from "src/views/app/containers/metricsTimeManager";
import RequireLogin from "src/views/login/requireLogin";
import { ThrottleNotificationBar } from "src/views/shared/components/alertBar/alertBar";

import "./layout.styl";
import "./layoutPanel.styl";

import TenantDropdown from "../../components/tenantDropdown/tenantDropdown";
import { LicenseNotification } from "../licenseNotification/licenseNotification";

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
      if (typeof this.contentRef.current.scrollTo === "function") {
        this.contentRef.current.scrollTo(0, 0);
      }
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
                <LicenseNotification />
                <LoginIndicator />
              </Right>
            </GlobalNavigation>
          </div>
          <div className="layout-panel__navigation-bar">
            <PageHeader>
              <Text textType={TextTypes.Heading2} noWrap>
                {getDataFromServer().FeatureFlags.is_observability_service
                  ? "(Obs Service) "
                  : ""}
                {clusterName || `Cluster id: ${clusterId || ""}`}
              </Text>
              <Badge text={clusterVersion} />
              <TenantDropdown />
            </PageHeader>
          </div>
          <ThrottleNotificationBar />
          <div className="layout-panel__body">
            <div className="layout-panel__sidebar">
              <NavigationBar />
            </div>
            <div ref={this.contentRef} className="layout-panel__content">
              <ErrorBoundary key={this.props.location.pathname}>
                {this.props.children}
              </ErrorBoundary>
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
    clusterVersion: clusterVersionLabelSelector(state),
    clusterId: clusterIdSelector(state),
  };
};

export default withRouter(connect(mapStateToProps)(Layout));
