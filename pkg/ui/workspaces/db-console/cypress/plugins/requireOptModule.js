// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const OPT_NODE_MODULES_PATH = "./opt/node_modules";
const path = require('path');

/**
 * @description Resolves module loading from custom `node_modules` location.
 * It is required only for .js files which aren't processed by Webpack.
 * @param module {string} module name to load from ./opt/node_modules
 * @return {*} required module
 */
function requireOptModule(module) {
  return require(path.resolve(OPT_NODE_MODULES_PATH, module));
}

module.exports = requireOptModule;
module.exports.OPT_NODE_MODULES_PATH = OPT_NODE_MODULES_PATH;
