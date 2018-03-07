import React from "react";

import step1 from "assets/nodeMapSteps/1-getLicense.svg";
import step2 from "assets/nodeMapSteps/2-setKey.svg";
import step3 from "assets/nodeMapSteps/3-seeMap.png";

import {
  NodeCanvasContainerOwnProps,
} from "src/views/clusterviz/containers/map/nodeCanvasContainer";
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
            <h1 className="need-license-blurb__header">Get a Clearer View of Your Cluster</h1>
            <p className="need-license-blurb__text">
              The Node Map shows your nodes on a map, grouped by locality.
              To enable it for your cluster, all you need is an
              {" "}<a href={LICENSE_DOCS_URL}>Enterprise license</a>{" "}
              and some configuration.
            </p>
          </div>
          <a href={TRIAL_LICENSE_URL} className="need-license-blurb__trial-link">
            GET A 30-DAY ENTERPRISE TRIAL
          </a>
        </div>
        <div className="need-license-steps">
          <Step num={1} text="Get a trial license delivered straight to your inbox." img={step1} />
          <Step num={2} text="Activate the trial license with two simple SQL commands." img={step2} />
          <Step num={3} text="Come back to this page and check out your cluster's Node Map." img={step3} />
        </div>
      </section>
    );
  }
}

function Step(props: { num: number, text: string, img: string }) {
  return (
    <div className="license-step">
      <img src={props.img} className="license-step__image" />
      <div className="license-step__text">
        <span className="license-step__stepnum">Step {props.num}:</span>{" "}
        {props.text}
      </div>
    </div>
  );
}
