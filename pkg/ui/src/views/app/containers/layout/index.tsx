import React from "react";
import { Helmet } from "react-helmet";
import { RouterState } from "react-router";

import NavigationBar from "src/views/app/components/layoutSidebar";
import TimeWindowManager from "src/views/app/containers/timewindow";
import AlertBanner from "src/views/app/containers/alertBanner";
import RequireLogin from "src/views/login/requireLogin";

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
        <Helmet titleTemplate="%s | Cockroach Console" defaultTitle="Cockroach Console" />
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
