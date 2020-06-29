#!/bin/bash

set -euo pipefail

export FILEBEAT_CONFIG_PATH=/tmp/filebeat.yml

# Install filebeat service and run at startup
echo "Installing Filebeat..."
# Should be latest version of Filebeat (not hard requirement)
curl -L -o filebeat-amd64.deb https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-7.7.1-amd64.deb
dpkg -i filebeat-amd64.deb
systemctl enable filebeat

# Copy over config file 
echo "Copying config..."
cp $FILEBEAT_CONFIG_PATH /etc/filebeat/filebeat.yml
# Filebeat requires that the core config file have root ownership
chown root /etc/filebeat/filebeat.yml

echo "Restarting filebeat..."
# Restart filebeat service
systemctl restart filebeat
