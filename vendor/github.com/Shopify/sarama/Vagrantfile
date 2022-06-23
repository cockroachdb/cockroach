# We have 5 * 192MB ZK processes and 5 * 320MB Kafka processes => 2560MB
MEMORY = 3072

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/bionic64"

  config.vm.provision :shell, path: "vagrant/provision.sh"

  config.vm.network "private_network", ip: "192.168.100.67"

  config.vm.provider "virtualbox" do |v|
    v.memory = MEMORY
  end
end
