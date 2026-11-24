// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Badge, useCluster, useClusterLabel } from "@cockroachlabs/cluster-ui";
import React, { useEffect, useRef } from "react";
import { Helmet } from "react-helmet";
import { useLocation } from "react-router-dom";

import {
  GlobalNavigation,
  CockroachLabsLockupIcon,
  Left,
  Right,
  PageHeader,
  Text,
  TextTypes,
} from "src/components";
import { AnalyticsProvider } from "src/contexts/analyticsProvider";
import { TimezoneProvider } from "src/contexts/timezoneProvider";
import { getDataFromServer } from "src/util/dataFromServer";
import ErrorBoundary from "src/views/app/components/errorMessage/errorBoundary";
import NavigationBar from "src/views/app/components/layoutSidebar";
import LoginIndicator from "src/views/app/components/loginIndicator";
import AlertBanner from "src/views/app/containers/alertBanner";
import { HealthMonitor } from "src/views/app/containers/healthMonitor";
import TimeWindowManager from "src/views/app/containers/metricsTimeManager";
import RequireLogin from "src/views/login/requireLogin";
import { ThrottleNotificationBar } from "src/views/shared/components/alertBar/alertBar";

import "./layout.scss";
import "./layoutPanel.scss";

import TenantDropdown from "../../components/tenantDropdown/tenantDropdown";
import { LicenseNotification } from "../licenseNotification/licenseNotification";

interface LayoutProps {
  children?: React.ReactNode;
}

/**
 * Defines the main layout of all admin ui pages. This includes static
 * navigation bars and footers which should be present on every page.
 *
 * Individual pages provide their content via react-router.
 */
function Layout({ children }: LayoutProps): React.ReactElement {
  const { clusterName, clusterVersion } = useClusterLabel();
  const { data } = useCluster();
  const clusterId = data?.cluster_id;
  const location = useLocation();
  const contentRef = useRef<HTMLDivElement>(null);

  // `react-router` doesn't handle scroll restoration (https://reactrouter.com/react-router/web/guides/scroll-restoration)
  // and when location changed with react-router's Link it preserves scroll position wherever it is.
  // AdminUI layout keeps left and top panels at fixed position on screen and has internal scrolling for content div
  // element which has to be scrolled back to top with navigation change.
  useEffect(() => {
    if (
      contentRef.current &&
      typeof contentRef.current.scrollTo === "function"
    ) {
      contentRef.current.scrollTo(0, 0);
    }
  }, [location.pathname]);

  return (
    <RequireLogin>
      <TimezoneProvider>
        <AnalyticsProvider>
          <Helmet
            titleTemplate="%s | Cockroach Console"
            defaultTitle="Cockroach Console"
          />
          <TimeWindowManager />
          <HealthMonitor />
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
              <div ref={contentRef} className="layout-panel__content">
                <ErrorBoundary key={location.pathname}>
                  {children}
                </ErrorBoundary>
              </div>
            </div>
          </div>
        </AnalyticsProvider>
      </TimezoneProvider>
    </RequireLogin>
  );
}

export default Layout;
