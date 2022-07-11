import { defineConfig } from "cypress";

export default defineConfig({
  e2e: {
    setupNodeEvents(on, config) {
      config.baseUrl = "http://localhost:8084";
      // implement node event listeners here
    },
  },
});
