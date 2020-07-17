#!/bin/bash

set -euo pipefail

export FILEBEAT_CONFIG_PATH=/tmp/filebeat.yml

# Download the Public Signing Key for the Beats APT repository
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add -

# Install prerequisite package
apt-get install apt-transport-https

# Save APT repo to disk
echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-7.x.list

# Install Filebeat via apt-get and run at startup
echo "Installing filebeat..."
apt-get update && apt-get install filebeat

# Copy over config file 
echo "Copying config..."
cp $FILEBEAT_CONFIG_PATH /etc/filebeat/filebeat.yml
# Filebeat requires that the core config file have root ownership
chown root:root /etc/filebeat/filebeat.yml

# Enable Filebeat service to run at startup
systemctl enable filebeat

# Restart Filebeat service
echo "Restarting filebeat..."
systemctl restart filebeat
