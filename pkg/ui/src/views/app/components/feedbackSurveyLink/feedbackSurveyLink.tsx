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

  return clusterId && singleVersion ? (
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
  ) : (
    <></>
  );
};

export default FeedBackSurveyLink;
