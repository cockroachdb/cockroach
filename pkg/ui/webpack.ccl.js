"use strict";

const path = require("path");

const webpackConfig = require("./webpack.app.js");

module.exports = webpackConfig("distccl", path.resolve(__dirname, "ccl"));
