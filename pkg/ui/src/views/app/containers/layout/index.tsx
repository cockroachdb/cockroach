import React from "react";
import _ from "lodash";
import { RouterState } from "react-router";
import { StickyContainer } from "react-sticky";

import { TitledComponent } from "src/interfaces/layout";
import NavigationBar from "src/views/app/components/layoutSidebar";
import TimeWindowManager from "src/views/app/containers/timewindow";
import AlertBanner from "src/views/app/containers/alertBanner";

function isTitledComponent(obj: Object | TitledComponent): obj is TitledComponent {
  return obj && _.isFunction((obj as TitledComponent).title);
}

/**
 * Defines the main layout of all admin ui pages. This includes static
 * navigation bars and footers which should be present on every page.
 *
 * Individual pages provide their content via react-router.
 */
export default class extends React.Component<RouterState, {}> {
  render() {
    // Responsibility for rendering a title is decided based on the route;
    // specifically, the most specific current route for which that route's
    // component implements a "title" method.
    const { routes, children } = this.props;
    let title: React.ReactElement<any>;

    for (let i = routes.length - 1; i >= 0; i--) {
      const component: Object | TitledComponent = routes[i].component;
      if (isTitledComponent(component)) {
        title = component.title(this.props);
        break;
      }
    }

    return <div>
      <TimeWindowManager/>
      <AlertBanner/>
      <NavigationBar/>
      <StickyContainer className="page">
        {
          // TODO(mrtracy): The title can be moved down to individual pages,
          // it is not always the top element on the page (for example, on
          // pages with a back button).
          !!title
            ? <section className="section"><h1>{ title }</h1></section>
            : null
        }
        { children }
      </StickyContainer>
    </div>;
  }
}
