import React from "react";

import step1Img from "assets/nodeMapSteps/1-getLicense.png";
import step2Img from "assets/nodeMapSteps/2-setKey.svg";
import step3Img from "assets/nodeMapSteps/3-seeMap.png";

import {
  NodeCanvasContainerOwnProps,
} from "src/views/clusterviz/containers/map/nodeCanvasContainer";
import { NODE_MAP_DOCS_URL } from "src/views/clusterviz/components/instructionsBox";
import "./needEnterpriseLicense.styl";

const LICENSE_DOCS_URL = "https://www.cockroachlabs.com/docs/stable/enterprise-licensing.html";
const TRIAL_LICENSE_URL = "https://www.cockroachlabs.com/pricing/start-trial/";

// This takes the same props as the NodeCanvasContainer which it is swapped out with.
export default class NeedEnterpriseLicense extends React.Component<NodeCanvasContainerOwnProps> {
  render() {
    return (
      <section className="need-license">
        <div className="need-license-blurb">
          <div>
            <h1 className="need-license-blurb__header">View the Node Map</h1>
            <p className="need-license-blurb__text">
              The Node Map shows the geographical layout of your cluster, along
              with metrics and health indicators. To enable the Node Map,
              request an <a href={LICENSE_DOCS_URL}>Enterprise trial license</a> and refer to
              this <a href={NODE_MAP_DOCS_URL}>configuration guide</a>.
            </p>
          </div>
          <a href={TRIAL_LICENSE_URL} className="need-license-blurb__trial-link">
            GET A 30-DAY ENTERPRISE TRIAL
          </a>
        </div>
        <div className="need-license-steps">
          <Step num={1} img={step1Img}>
            <a href={TRIAL_LICENSE_URL}>Get a trial license</a> delivered straight to your inbox.
          </Step>
          <Step num={2} img={step2Img}>
            Activate the trial license with two simple SQL commands.
          </Step>
          <Step num={3} img={step3Img}>
            Refer this <a href={NODE_MAP_DOCS_URL}>configuration guide</a> to configure the Node Map.
          </Step>
        </div>
      </section>
    );
  }
}

function Step(props: { num: number, img: string, children: React.ReactNode }) {
  return (
    <div className="license-step">
      <img src={props.img} className="license-step__image" />
      <div className="license-step__text">
        <span className="license-step__stepnum">Step {props.num}:</span>{" "}
        {props.children}
      </div>
    </div>
  );
}
