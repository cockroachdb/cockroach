#!/usr/bin/env zsh

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

write_teamcity_config() {
    sudo -u agent tee /Users/agent/teamcity/conf/buildAgent.properties <<EOF
serverUrl=https://teamcity.cockroachdb.com
name=
workDir=../work
tempDir=../temp
systemDir=../system
EOF
}

write_plist_file() {
    sudo -u agent tee /Users/agent/Library/LaunchAgents/jetbrains.teamcity.BuildAgent.plist <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>UserName</key>
	<string>agent</string>
	<key>WorkingDirectory</key>
	<string>/Users/agent/teamcity</string>
	<key>SessionCreate</key>
	<true/>
	<key>Label</key>
	<string>jetbrains.teamcity.BuildAgent</string>
	<key>OnDemand</key>
	<false/>
	<key>KeepAlive</key>
	<true/>
	<key>LowPriorityBackgroundIO</key>
	<false/>
	<key>LowPriorityIO</key>
	<false/>
	<key>ProcessType</key>
	<string>Interactive</string>
	<key>ProgramArguments</key>
	<array>
		<string>/Users/agent/teamcity/bin/agent.sh</string>
		<string>run</string>
	</array>
	<key>EnvironmentVariables</key>
	<dict>
		<key>JAVA_HOME</key>
		<string>/opt/homebrew/opt/openjdk@11</string>
		<key>PATH</key>
		<string>/opt/homebrew/opt/openjdk@11/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
	</dict>
	<key>RunAtLoad</key>
	<true/>
	<key>StandardErrorPath</key>
	<string>/Users/agent/teamcity/logs/launchd.err.log</string>
	<key>StandardOutPath</key>
	<string>/Users/agent/teamcity/logs/launchd.out.log</string>
</dict>
</plist>
EOF
}

change_password() {
    tee /tmp/change_password <<'EOF'
#!/usr/bin/expect -f
set username [lindex $argv 0]
set password [lindex $argv 1]
spawn passwd $username
expect "New password:"
send "$password\r"
expect "Retype new password:"
send "$password\r"
expect "################################### WARNING ###################################"
expect "# This tool does not update the login keychain password.                      #"
expect "# To update it, run `security set-keychain-password` as the user in question, #"
expect "# or as root providing a path to such user's login keychain.                  #"
expect "###############################################################################"
expect eof
EOF
    sudo expect /tmp/change_password $1 $2
    rm -f /tmp/change_password
}

set_kcpassword() {
    tee /tmp/set_kcpassword.py <<'EOF'
#!/usr/bin/env python3

# Port of Gavin Brock's Perl kcpassword generator to Python, by Tom Taylor
# <tom@tomtaylor.co.uk>.
# Perl version: http://www.brock-family.org/gavin/perl/kcpassword.html
# https://github.com/timsutton/osx-vm-templates/blob/master/scripts/support/set_kcpassword.py

import sys
import os

def kcpassword(passwd):
    # The magic 11 bytes - these are just repeated
    # 0x7D 0x89 0x52 0x23 0xD2 0xBC 0xDD 0xEA 0xA3 0xB9 0x1F
    key = [125,137,82,35,210,188,221,234,163,185,31]
    key_len = len(key)

    passwd = [ord(x) for x in list(passwd)]
    # pad passwd length out to an even multiple of key length
    r = len(passwd) % key_len
    if (r > 0):
        passwd = passwd + [0] * (key_len - r)

    for n in range(0, len(passwd), len(key)):
        ki = 0
        for j in range(n, min(n+len(key), len(passwd))):
            passwd[j] = passwd[j] ^ key[ki]
            ki += 1

    passwd = [chr(x) for x in passwd]
    return "".join(passwd)

if __name__ == "__main__":
    passwd = kcpassword(sys.argv[1])
    fd = os.open('/etc/kcpassword', os.O_WRONLY | os.O_CREAT, 0o600)
    file = os.fdopen(fd, 'w')
    file.write(passwd)
    file.close()
EOF
    sudo python3 /tmp/set_kcpassword.py $1
    rm -f /tmp/set_kcpassword.py
}

/opt/homebrew/bin/brew install openjdk@11

sudo sysadminctl -addUser agent

PASSWORD=$(openssl rand -base64 18)
change_password agent "$PASSWORD"
set_kcpassword "$PASSWORD"
sudo /usr/bin/defaults write /Library/Preferences/com.apple.loginwindow autoLoginUser agent

sudo su - agent <<'EOF'
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@11' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
EOF

sudo su - agent <<'EOF'
# NB: This file should be automatically loaded by zsh, but it's not for some
# reason. I can't figure out why.
set -euxo pipefail
source ~/.zshrc
curl -fsSL https://teamcity.cockroachdb.com/update/buildAgent.zip -o buildAgent.zip
unzip buildAgent.zip -d teamcity
rm buildAgent.zip
sed -i -e "s/wrapper.java.command=.*/wrapper.java.command=java/" teamcity/launcher/conf/wrapper.conf
mkdir -p ~/Library/LaunchAgents
mkdir -p teamcity/logs
EOF

write_teamcity_config

sudo su - agent <<'EOF'
set -euxo pipefail
source ~/.zshrc
# Ref: https://www.jetbrains.com/help/teamcity/start-teamcity-agent.html#Automatic+Agent+Start+Under+macOS
teamcity/bin/agent.sh start
until grep -q 'Updating agent parameters on the server' teamcity/logs/teamcity-agent.log
do
  echo .
  sleep 5
done
teamcity/bin/agent.sh stop force
sleep 5
rm -f teamcity/logs/*
EOF

write_teamcity_config
write_plist_file
