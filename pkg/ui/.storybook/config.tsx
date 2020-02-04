import React from "react";
import { configure, addDecorator } from "@storybook/react";

// Import global styles here
import "nvd3/build/nv.d3.min.css";
import "react-select/dist/react-select.css";
import "antd/es/tooltip/style/css";
import "styl/app.styl";

const req = require.context("../src/", true, /.stories.tsx$/);

function loadStories() {
  req.keys().forEach(filename => req(filename));
}

addDecorator(storyFn => (
  <div style={{padding: "24px"}}>
    {storyFn()}
  </div>
));

configure(loadStories, module);
