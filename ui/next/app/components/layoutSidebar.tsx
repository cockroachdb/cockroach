/// <reference path="../../typings/main.d.ts" />

import * as React from "react";
import { Link, RouterOnContext } from "react-router";
import * as Icons from "./icons.ts";

function trustIcon(iconSvg: string) {
  "use strict";
  return {__html: iconSvg};
}

interface IconLinkProps {
  to: string;
  className?: string;
  icon?: string;
  title?: string;
}

interface IconLinkContext {
  router: RouterOnContext;
}

/**
 * IconLink creats a react router Link which contains both a graphical icon and
 * a string title.
 *
 * TODO(mrtracy): During construction of 'next', we are explicitly avoiding any
 * CSS changes from the existing ui. After publishing 'next', we should
 * restructure the css of these links slightly so that the 'active' class is
 * present directly on the <a> element, instead of the containing <li> element;
 * that will allow us to use the react-router's <Link> class to provide the
 * activeClass, rather than the logic below.
 */
class IconLink extends React.Component<IconLinkProps, {}> {
  static contextTypes = {
    router: React.PropTypes.object,
  };

  static defaultProps = {
    className: "normal",
  };

  context: IconLinkContext;

  render() {
    let {to, className, icon, title} = this.props;
    if (this.context.router.isActive(to)) {
      if (className) {
        className += " active";
      } else {
        className = "active";
      }
    }
    return <li className={className}>
      <Link to={to}>
        <div className=".image-container"
             dangerouslySetInnerHTML={trustIcon(icon)}/>
        <div>{title}</div>
      </Link>
    </li>;
	}
}

/**
 * SideBar represents the static navigation sidebar available on all pages. It
 * displays a number of graphic icons representing available pages; the icon of
 * the page which is currently active will be highlighted.
 */
export default class extends React.Component<{}, {}> {
  render() {
    return <div id="header">
      <header>
        <ul className="nav">
          <IconLink to="/" icon={Icons.cockroachIcon} className="cockroach"/>
          <IconLink to="/cluster" icon={Icons.clusterIcon} title="Cluster"/>
          <IconLink to="/nodes" icon={Icons.nodesIcon} title="Nodes"/>
          <IconLink to="/databases" icon={Icons.databaseIcon} title="Databases"/>
          <IconLink to="/help-us/reporting" icon={Icons.cockroachIconSmall} title="Help Us"/>
        </ul>
      </header>
    </div>;
  }
}
