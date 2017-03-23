import * as React from "react";
import _ from "lodash";
import { IInjectedProps } from "react-router";
import { StickyContainer } from "react-sticky";

import { TitledComponent } from "../interfaces/layout";
import NavigationBar from "../components/layoutSidebar";
import TimeWindowManager from "../containers/timewindow";
import Banner from "../containers/banner/bannerContainer";

function isTitledComponent(obj: Object | TitledComponent): obj is TitledComponent {
  return obj && _.isFunction((obj as TitledComponent).title);
}

/**
 * Defines the main layout of all admin ui pages. This includes static
 * navigation bars and footers which should be present on every page.
 *
 * Individual pages provide their content via react-router.
 */
export default class extends React.Component<IInjectedProps, {}> {
  render() {
    // Responsibility for rendering a title is decided based on the route;
    // specifically, the most specific current route for which that route's
    // component implements a "title" method.
    let { routes, children } = this.props;
    let title: React.ReactElement<any>;

    for (let i = routes.length - 1; i >= 0; i--) {
      let component: Object | TitledComponent = routes[i].component;
      if (isTitledComponent(component)) {
        title = component.title(this.props);
        break;
      }
    }

    return <div>
      <TimeWindowManager/>
      <NavigationBar/>
      <StickyContainer className="page">
        {
          // TODO(mrtracy): The title can be moved down to individual pages,
          // it is not always the top element on the page (for example, on
          // pages with a back button).
          !!title ? <section className="header">{ title }</section>
                  : null
        }
        <Banner/>
        { children }
      </StickyContainer>
    </div>;
  }
}
