# -*- mode: ruby -*-
# vi: set ft=ruby :

$provisioner = <<SCRIPT
  #!/bin/sh
  set -e -x

  sudo apt-get update -qq
  sudo apt-get install -qy python-software-properties
  sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
  sudo apt-get update -qq
  sudo apt-get install -y -qq gcc-4.8 g++-4.8 libjemalloc-dev libprotobuf-dev protobuf-compiler clang curl make git mercurial
  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.8 50
  sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 50

  # Go time.
  curl -s https://storage.googleapis.com/golang/go1.3.1.linux-amd64.tar.gz | sudo tar -v -C /usr/local -xz
  echo "export GOPATH=/home/vagrant/go" >> .bashrc
  echo "export PATH=/usr/local/go/bin:/home/vagrant/go/bin:\\$PATH" >> .bashrc
  echo "export LD_LIBRARY_PATH=/usr/local/lib:\\$LD_LIBRARY_PATH" >> .bashrc
SCRIPT

Vagrant.require_version '>= 1.5.0'
Vagrant.configure("2") do |config|
  # Every Vagrant virtual environment requires a box to build off of.
  config.vm.box = "precise64"
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  config.vm.synced_folder "../../../../", "/home/vagrant/go"

  config.vm.provision "shell", inline: $provisioner
end
