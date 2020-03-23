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



const cypressTypeScriptPreprocessor = require('./cy-ts-preprocessor');

/**
 * @type {Cypress.PluginConfig}
 */
module.exports = (on, config) => {
  on('file:preprocessor', cypressTypeScriptPreprocessor)
};
