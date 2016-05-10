/// <reference path="../../typings/main.d.ts" />
import * as React from "react";
import SideBar from "../components/layoutSidebar";
import Header from "../components/layoutHeader";

/**
 * LayoutProps are the props supported by the Layout class.
 */
interface LayoutProps<T, V> {
  main: React.ReactElement<T>;
  title: React.ReactElement<V>;
}

/**
 * Layout defines the main layout of all admin ui pages. This includes static
 * navigation bars and footers which should be present on every page.
 *
 * Individual pages provide their content via react-router, passing in named
 * components "main" and "title".
 */
class Layout<T, V> extends React.Component<LayoutProps<T, V>, {}> {
  render() {
    return <div id="content">
      <SideBar/>
      <div id="page-container">
        <div id="root">
          <div className="page">
            <Header>
            {this.props.title}
            </Header>
            {this.props.main}
          </div>
        </div>
      </div>
    </div>;
  }
}

export default Layout;
