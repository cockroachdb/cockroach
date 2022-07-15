# WARNING
# This Dockerfile must be built from the root of cockroachdb after the cockroach
# binary has been built. Running it from other directories will cause errors.
FROM cypress/browsers:node16.14.2-slim-chrome100-ff99-edge

# Install CRDB prerequisites
RUN apt install libresolv-wrapper

# Add the compiled CRDB binary
ADD ./cockroach /usr/bin/cockroach

# Switch to the non-root user
USER node

## Install e2e test dependencies to a scratch directory during the build phase
## so they don't get overwritten when tests are mounted
#WORKDIR /scratch
#ADD ./pkg/ui/.yarnrc ./
#ADD ./pkg/ui/workspaces/e2e-tests/package.json ./pkg/ui/workspaces/e2e-tests/yarn.lock ./
#RUN yarn install --ignore-scripts

WORKDIR /cypress
ADD ./pkg/ui/.yarnrc ./
RUN ls -l
COPY ./pkg/ui/yarn-vendor ./yarn-vendor/
RUN ls -l
RUN ls -l ./yarn-vendor/
COPY ./pkg/ui/workspaces/e2e-tests ./
RUN ls -l
RUN yarn install

# Run tests during the execution phase, assuming pkg/ui/workspaces/e2e-tests was
# mounted to /cypress during the build. This allows the dependencies to be
# cached within the built container, but the tests to be iterated on with
# minimal overhead.
ENV COCKROACH=/usr/bin/cockroach
CMD ["/usr/bin/yarn", "test"]
