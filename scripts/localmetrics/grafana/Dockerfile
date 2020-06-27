# NB: want master to pick up GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH:
# https://github.com/grafana/grafana/pull/25595
# TODO(tbg): pin a release once the above is released to avoid random breakage.
FROM grafana/grafana:master

ENV GF_INSTALL_PLUGINS grafana-clock-panel,briangann-gauge-panel,natel-plotly-panel,grafana-simple-json-datasource
# Set up admin login
ENV GF_SECURITY_ADMIN_PASSWORD x
# Disable anonymous login for now - when we have great auto-generated dashboards
# we can enable it, but as is a shitty dashboard you can't edit isn't a great
# place to land by default.
#ENV GF_AUTH_ANONYMOUS_ENABLED true
#ENV GF_AUTH_ANONYMOUS_ORG_NAME Main Org.
ENV GF_USERS_ALLOW_SIGN_UP false
ENV GF_DASHBOARDS_JSON_ENABLED true
ENV GF_DASHBOARDS_JSON_PATH ./docker-compose.d/grafana
ENV GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH /var/lib/grafana/dashboards/home.json

COPY ./postgres.yml /etc/grafana/provisioning/datasources/postgres.yml
COPY ./dashboards.yml /etc/grafana/provisioning/dashboards/dashboards.yml
# /var/lib/grafana/dashboards/ is mounted in from the outside, to allow editing
# while Grafana is running.
# COPY ./dashboards /var/lib/grafana/dashboards
