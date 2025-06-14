// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { defineConfig } from "cypress";

const DOCKER_OVERRIDES: Partial<Cypress.UserConfigOptions["e2e"]> = {
  baseUrl: "https://crdbhost:8080",
  reporter: "teamcity",
  downloadsFolder: "/artifacts/cypress/scale/downloads",
  screenshotsFolder: "/artifacts/cypress/scale/screenshots",
  videosFolder: "/artifacts/cypress/scale/videos",
};

export default defineConfig({
  e2e: {
    baseUrl: "http://localhost:8080",
    video: true,
    specPattern: "cypress/scale/**/*.{js,jsx,ts,tsx}",
    numTestsKeptInMemory: 0,
    viewportWidth: 1920,
    viewportHeight: 1080,
    setupNodeEvents(on, config) {
      on("task", {
        logTiming: (data) => {
          console.log(
            `📊 Measured Timing: ${data.renderName} - ${data.duration.toFixed(
              2,
            )}ms`,
          );
          return null;
        },
      });
      return config;
    },
    // Override some settings when running in Docker
    ...(process.env.IS_DOCKER ? DOCKER_OVERRIDES : {}),
  },
});
