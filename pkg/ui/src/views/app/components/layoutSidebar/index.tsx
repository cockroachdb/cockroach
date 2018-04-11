import classNames from "classnames";
import _ from "lodash";
import React from "react";
import PropTypes from "prop-types";
import { Link } from "react-router";
import { cockroachIcon } from "src/views/shared/components/icons";
import { trustIcon } from "src/util/trust";

import homeIcon from "!!raw-loader!assets/home.svg";
import metricsIcon from "!!raw-loader!assets/metrics.svg";
import databasesIcon from "!!raw-loader!assets/databases.svg";
import jobsIcon from "!!raw-loader!assets/jobs.svg";

interface IconLinkProps {
  icon?: string;
  title?: string;
  to: string;
  activeFor?: string | string[];
  onlyActiveOnIndex?: boolean;
  className?: string;
}

/**
 * IconLink creats a react router Link which contains both a graphical icon and
 * a string title.
 */
class IconLink extends React.Component<IconLinkProps, {}> {
  static defaultProps = {
    className: "normal",
    onlyActiveOnIndex: false,
  };

  static contextTypes = {
    router: PropTypes.object,
  };

  render() {
    const { icon, title, to, activeFor, onlyActiveOnIndex, className } = this.props;

    let isActive = false;
    if (!_.isNil(activeFor)) {
      const router = this.context.router;
      const options = typeof activeFor === "string" ? [activeFor] : activeFor;
      isActive = _.some(options, (opt) => router.isActive(opt, onlyActiveOnIndex));
    }
    const classOverrides = classNames({ active: isActive });

    return (
      <li className={className} >
        <Link
          to={to}
          activeClassName="active"
          onlyActiveOnIndex={onlyActiveOnIndex}
          className={classOverrides}
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
 * SideBar represents the static navigation sidebar available on all pages. It
 * displays a number of graphic icons representing available pages; the icon of
 * the page which is currently active will be highlighted.
 */
export default class extends React.Component<{}, {}> {
  render() {
    return <nav className="navigation-bar">
      <ul className="navigation-bar__list">
        <IconLink to="/overview" icon={homeIcon} title="Overview" activeFor="/node" />
        <IconLink to="/metrics" icon={metricsIcon} title="Metrics" />
        <IconLink to="/databases" icon={databasesIcon} title="Databases" activeFor="/database" />
        <IconLink to="/jobs" icon={jobsIcon} title="Jobs" />
      </ul>
      <ul className="navigation-bar__list navigation-bar__list--bottom">
        <IconLink to="/" icon={cockroachIcon} className="cockroach" />
      </ul>
    </nav>;
  }
}
