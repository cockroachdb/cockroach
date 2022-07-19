import { defineConfig } from "cypress";
import { mkdirSync } from "fs";

const DOCKER_OVERRIDES: Partial<Cypress.UserConfigOptions> = {
  // Don't remove other artifacts, in case other job phases included some already.
  trashAssetsBeforeRuns: false,
  downloadsFolder: "/artifacts/cypress/downloads",
  screenshotsFolder: "/artifacts/cypress/screenshots",
  videosFolder: "/artifacts/cypress/videos",
};

export default defineConfig({
  e2e: {
    baseUrl: "http://localhost:8080",
    setupNodeEvents(on, config) {
      // Implement custom node event listeners here
    },
    // Override some settings when running in TeamCity
    ...(
      process.env.IS_DOCKER
        ? DOCKER_OVERRIDES
        : {}
    ),
  },
});
