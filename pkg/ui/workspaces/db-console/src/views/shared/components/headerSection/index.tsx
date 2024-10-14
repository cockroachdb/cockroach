// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Link, RouteComponentProps, withRouter } from "react-router-dom";

import { Text, TextTypes } from "src/components";
import { trustIcon } from "src/util/trust";

import ArrowLeftIcon from "!!raw-loader!assets/arrowLeft.svg";
import "./headerSection.styl";

export interface HeaderSectionProps {
  title: string;
  navigationBackConfig?: {
    text: string;
    path: string;
  };
}

const HeaderSection: React.FC<
  HeaderSectionProps & RouteComponentProps
> = props => {
  const { navigationBackConfig, title } = props;
  return (
    <div className="header-section">
      {navigationBackConfig && (
        <div className="header-section__back-link">
          <span
            className="header-section__back-icon"
            dangerouslySetInnerHTML={trustIcon(ArrowLeftIcon)}
          />
          <Link to={navigationBackConfig.path}>
            {navigationBackConfig.text}
          </Link>
        </div>
      )}
      <div className="header-section__title">
        <Text textType={TextTypes.Heading3}>{title}</Text>
      </div>
    </div>
  );
};

export default withRouter(HeaderSection);
