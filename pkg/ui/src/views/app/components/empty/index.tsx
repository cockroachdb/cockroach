// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Button } from "antd";
import React from "react";
import heroBannerLp from "assets/heroBannerLp.png";
import "./styles.styl";

export interface IEmptyProps {
  title: string;
  description?: string;
  button?: React.ReactNode;
  backgroundImage?: string;
}

export default function Empty (props: IEmptyProps) {
  const {
    title,
    description = "Learn more about how to write queries.",
    button = "Learn more",
    backgroundImage = heroBannerLp,
  } = props;
  return (
    <div className="empty-container">
      <h2 className="empty-container__title">{title}</h2>
      <p className="empty-container__description">{description}</p>
      <Button className="empty-container__button" type="primary">{button}</Button>
      <img className="empty-container__background" src={backgroundImage} alt="Empty screen" />
    </div>
  );
}
