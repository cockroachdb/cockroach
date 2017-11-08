"use strict";

const path = require("path");

const webpackConfig = require("./webpack.common.js");

module.exports = webpackConfig(path.resolve(__dirname, "ccl"));
