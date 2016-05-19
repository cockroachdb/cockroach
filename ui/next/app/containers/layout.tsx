/// <reference path="../../typings/main.d.ts" />
import * as React from "react";
import _ = require("lodash");
import { RouteComponentProps } from "react-router";

import { TitledComponent } from "../interfaces/layout";
import SideBar from "../components/layoutSidebar";
import Header from "../components/layoutHeader";
import TimeWindowManager from "../containers/timewindow";

function isTitledComponent(obj: any): obj is TitledComponent {
  return obj && _.isFunction(obj.title);
}

/**
 * Defines the main layout of all admin ui pages. This includes static
 * navigation bars and footers which should be present on every page.
 *
 * Individual pages provide their content via react-router.
 */
export default class extends React.Component<RouteComponentProps<any, any>, {}> {
  render() {
    // Responsibility for rendering a title is decided based on the route;
    // specifically, the most specific current route for which that route's
    // component implements a "title" method.
    let { routes, children } = this.props;
    let titleIndex = _.findLastIndex(routes, (r) => isTitledComponent(r.component));

    // We already know from findLastIndex that routes[titleIndex].component is
    // of type TitledComponent.
    let titleComponent = (titleIndex !== -1) ? (routes[titleIndex].component as any as TitledComponent) : null;
    let title = titleComponent ? titleComponent.title(this.props) : "";

    return <div id="content">
      <TimeWindowManager/>
      <SideBar/>
      <div id="page-container">
        <div id="root">
          <div className="page">
            <Header>
            { title }
            </Header>
            { children }
          </div>
        </div>
      </div>
    </div>;
  }
}
