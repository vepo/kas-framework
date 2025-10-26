Maquinas MAGALU BV4-8-100

sudo zypper refresh && sudo zypper install git-core zip unzip
ssh-keygen -t ed25519 -C "victor.perticarrari@gmail.com"
cat ~/.ssh/id_ed25519.pub
git clone git@github.com:vepo/maestro.git
sudo zypper install docker docker-compose docker-compose-switch
sudo systemctl enable docker
sudo usermod -G docker -a $USER
newgrp docker
sudo systemctl restart docker

## Brokers

lsblk
sudo mkfs.ext4 /dev/vdb
sudo mkdir /mnt/docker-data
sudo mount /dev/vdb /mnt/docker-data

### Create test environment
cd maestro
./scripts/start-kafka

## Streams
curl -s "https://get.sdkman.io" | bash
source "/home/opensuse/.sdkman/bin/sdkman-init.sh"
sdk install java 21.0.2-open
sdk install maven 3.9.9
cd maestro/
mvn clean package

sudo zypper in go go-doc
export PATH=$PATH:/home/$USER/go/bin
echo 'export PATH=$PATH:/home/$USER/go/bin' >> ~/.bashrc
go install github.com/hangxie/parquet-tools@latest


### Execute test
cd maestro
./scripts/start-test.sh 


Cloud 2

Streams BV8-16-100 
Brokers BV4-8-100