import React from "react";
import { Helmet } from "react-helmet";
import { RouterState } from "react-router";
import { StickyContainer } from "react-sticky";

import NavigationBar from "src/views/app/components/layoutSidebar";
import TimeWindowManager from "src/views/app/containers/timewindow";
import AlertBanner from "src/views/app/containers/alertBanner";

/**
 * Defines the main layout of all admin ui pages. This includes static
 * navigation bars and footers which should be present on every page.
 *
 * Individual pages provide their content via react-router.
 */
export default class extends React.Component<RouterState, {}> {
  render() {
    return <div>
      <Helmet titleTemplate="%s | Cockroach Console" defaultTitle="Cockroach Console" />
      <TimeWindowManager/>
      <AlertBanner/>
      <NavigationBar/>
      <StickyContainer className="page">
        { this.props.children }
      </StickyContainer>
    </div>;
  }
}
