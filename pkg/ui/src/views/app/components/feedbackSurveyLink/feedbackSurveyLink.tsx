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
  return (
    <div className="feedback-survey-link">
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
