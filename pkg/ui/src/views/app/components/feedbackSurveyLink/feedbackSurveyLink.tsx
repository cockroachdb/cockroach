// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { useSelector } from "react-redux";

import externalLinkIcon from "!!raw-loader!assets/external-link.svg";
import { trustIcon } from "src/util/trust";
import {
  singleVersionSelector,
  clusterIdSelector,
} from "../../../../redux/nodes";
import "./feedbackSurveyLink.styl";

const FeedBackSurveyLink = () => {
  const singleVersion = useSelector(singleVersionSelector);
  const clusterId = useSelector(clusterIdSelector);
  const feedbackLink = new URL("https://www.cockroachlabs.com/survey/");
  feedbackLink.searchParams.append("clusterId", clusterId);
  feedbackLink.searchParams.append("clusterVersion", singleVersion);
  if (!clusterId || !singleVersion) {
    return <></>;
  }
  return (
    <div
      className="feedback-survey-link"
      onClick={() => window.open(feedbackLink.toString())}
    >
      <div
        className="image-container"
        title="Share Feedback"
        dangerouslySetInnerHTML={trustIcon(externalLinkIcon)}
      />
      <div className="feedback-survey-link__title">Share feedback</div>
    </div>
  );
};

export default FeedBackSurveyLink;
