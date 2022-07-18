# WARNING
# This Dockerfile must be built from the root of cockroachdb after the cockroach
# binary has been built. Running it from other directories will cause errors.
FROM cypress/browsers:node16.14.2-slim-chrome100-ff99-edge

# Install CRDB prerequisites
RUN apt install libresolv-wrapper

# Add the compiled CRDB binary
ADD ./cockroach /usr/bin/cockroach

# Switch to the non-root user included by default
USER node

# Add the tests and install NPM dependencies
WORKDIR /cypress
COPY --chown=node ./pkg/ui/yarn-vendor ./yarn-vendor/
COPY --chown=node ./pkg/ui/workspaces/e2e-tests ./
ADD ./pkg/ui/.yarnrc ./
RUN yarn install

ENV IS_DOCKER=1
ENV COCKROACH=/usr/bin/cockroach
CMD yarn test
