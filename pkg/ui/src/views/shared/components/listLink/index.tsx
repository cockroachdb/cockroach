/**
 * TODO(mrtracy): During construction of 'next', we are explicitly avoiding any
 * CSS changes from the existing ui. After publishing 'next', we should
 * restructure the css of these links slightly so that the 'active' class is
 * present directly on the <a> element, instead of the containing <li> element;
 * that will allow us to use the react-router's <Link> class to provide the
 * activeClass, rather than the logic below.
 */

import React from "react";
import PropTypes from "prop-types";
import { Link, InjectedRouter, RouterState } from "react-router";

export interface LinkProps {
  to: string;
  onlyActiveOnIndex?: boolean;
  className?: string;
}

export class ListLink extends React.Component<LinkProps, {}> {
  // TODO(mrtracy): Switch this, and the other uses of contextTypes, to use the
  // 'withRouter' HoC after upgrading to react-router 4.x.
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };

  static defaultProps = {
    className: "normal",
    onlyActiveOnIndex: false,
  };

  context: { router: InjectedRouter & RouterState; };

  render() {
    let { to, className, onlyActiveOnIndex, children } = this.props;
    if (this.context.router.isActive(to, onlyActiveOnIndex)) {
      if (className) {
        className += " active";
      } else {
        className = "active";
      }
    }
    return <li className={className}>
      <Link to={to}>
        { children }
      </Link>
    </li>;
  }
}

export class IndexListLink extends React.Component<LinkProps, {}> {
  render() {
    return <ListLink {...this.props} onlyActiveOnIndex={true} />;
  }
}
