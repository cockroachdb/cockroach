// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import step1Img from "assets/nodeMapSteps/1-getLicense.png";
import step2Img from "assets/nodeMapSteps/2-setKey.svg";
import step3Img from "assets/nodeMapSteps/3-seeMap.png";

import { NodeCanvasContainerOwnProps } from "src/views/clusterviz/containers/map/nodeCanvasContainer";
import * as docsURL from "src/util/docs";
import "./needEnterpriseLicense.styl";

// This takes the same props as the NodeCanvasContainer which it is swapped out with.

function Step(props: { num: number; img: string; children: React.ReactNode }) {
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

export default class NeedEnterpriseLicense extends React.Component<NodeCanvasContainerOwnProps> {
  render() {
    return (
      <section className="need-license">
        <div className="need-license-blurb">
          <div>
            <h1 className="base-heading need-license-blurb__header">
              View the Node Map
            </h1>
            <p className="need-license-blurb__text">
              The Node Map shows the geographical layout of your cluster, along
              with metrics and health indicators. To enable the Node Map,
              request an{" "}
              <a
                href={docsURL.enterpriseLicensing}
                target="_blank"
                rel="noreferrer"
              >
                Enterprise trial license
              </a>{" "}
              and refer to this{" "}
              <a href={docsURL.enableNodeMap} target="_blank" rel="noreferrer">
                configuration guide
              </a>
              .
            </p>
          </div>
          <a
            href={docsURL.startTrial}
            className="need-license-blurb__trial-link"
            target="_blank"
            rel="noreferrer"
          >
            GET A 30-DAY ENTERPRISE TRIAL
          </a>
        </div>
        <div className="need-license-steps">
          <Step num={1} img={step1Img}>
            <a href={docsURL.startTrial} target="_blank" rel="noreferrer">
              Get a trial license
            </a>{" "}
            delivered straight to your inbox.
          </Step>
          <Step num={2} img={step2Img}>
            Activate the trial license with two simple SQL commands.
          </Step>
          <Step num={3} img={step3Img}>
            Follow{" "}
            <a href={docsURL.enableNodeMap} target="_blank" rel="noreferrer">
              this guide
            </a>{" "}
            to configure localities and locations.
          </Step>
        </div>
      </section>
    );
  }
}
