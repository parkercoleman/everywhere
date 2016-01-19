Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.network "forwarded_port", guest: 80, host: 8081
  config.vm.network "public_network"

  config.vm.provider "virtualbox" do |vb|
    vb.name = "otbp"
    vb.memory = 4096
    vb.cpus = 2
  end

  config.vm.provision "shell", inline: <<-SHELL
    sudo apt-get update
    apt-get install -y postgresql
    apt-get install -y python3-pip
    sudo apt-get install -y libgeos-dev
    pip3 install -r /vagrant/requirements.txt
  SHELL
end
