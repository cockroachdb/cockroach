import classNames from "classnames";
import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import { Link } from "react-router";

import { AdminUIState } from "src/redux/state";
import { LoginState } from "src/redux/login";
import { cockroachIcon } from "src/views/shared/components/icons";
import { trustIcon } from "src/util/trust";

import homeIcon from "!!raw-loader!assets/home.svg";
import metricsIcon from "!!raw-loader!assets/metrics.svg";
import databasesIcon from "!!raw-loader!assets/databases.svg";
import jobsIcon from "!!raw-loader!assets/jobs.svg";

interface IconLinkProps {
  icon: string;
  title?: string;
  to: string;
  activeFor?: string | string[];
  className?: string;
}

/**
 * IconLink creats a react router Link which contains both a graphical icon and
 * a string title.
 */
class IconLink extends React.Component<IconLinkProps, {}> {
  static defaultProps: Partial<IconLinkProps> = {
    className: "normal",
    activeFor: [],
  };

  static contextTypes = {
    router: PropTypes.object,
  };

  render() {
    const { icon, title, to, activeFor, className } = this.props;

    const router = this.context.router;
    const linkRoutes = [to].concat(activeFor);
    const isActive = _.some(linkRoutes, (route) => router.isActive(route, false));
    return (
      <li className={className} >
        <Link
          to={to}
          className={classNames({ active: isActive })}
        >
          <div className="image-container"
               dangerouslySetInnerHTML={trustIcon(icon)}/>
          <div>{title}</div>
        </Link>
      </li>
    );
  }
}

/**
 * Sidebar represents the static navigation sidebar available on all pages. It
 * displays a number of graphic icons representing available pages; the icon of
 * the page which is currently active will be highlighted.
 */

interface SidebarProps {
  loginState: LoginState;
}

class Sidebar extends React.Component<SidebarProps> {
  render() {
    return (
      <nav className="navigation-bar">
        <ul className="navigation-bar__list">
          <IconLink to="/overview" icon={homeIcon} title="Overview" activeFor="/node" />
          <IconLink to="/metrics" icon={metricsIcon} title="Metrics" />
          <IconLink to="/databases" icon={databasesIcon} title="Databases" activeFor="/database" />
          <IconLink to="/jobs" icon={jobsIcon} title="Jobs" />
        </ul>
        <div className="logged-in-user">
          Logged in as{" "}
          <span className="logged-in-user--name">{this.props.loginState.loggedInUser}</span>
        </div>
        <ul className="navigation-bar__list navigation-bar__list--bottom">
          <IconLink to="/" icon={cockroachIcon} className="cockroach" />
        </ul>
      </nav>
    );
  }
}

// tslint:disable-next-line:variable-name
const SidebarConnected = connect(
  (state: AdminUIState) => ({
    loginState: state.login,
  }),
)(Sidebar);

export default SidebarConnected;
