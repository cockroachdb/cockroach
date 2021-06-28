/// <reference types="cypress" />

// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
const requireOpt = require("./requireOptModule");
const cypressTypeScriptPreprocessor = require("./cy-ts-preprocessor");
const { addMatchImageSnapshotPlugin } = requireOpt("cypress-image-snapshot/plugin");

/**
 * @type {Cypress.PluginConfig}
 */
module.exports = (on, config) => {
  on("file:preprocessor", cypressTypeScriptPreprocessor);
  addMatchImageSnapshotPlugin(on, config);
};
