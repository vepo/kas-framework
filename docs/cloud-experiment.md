sudo zypper refresh
sudo zypper install git-core
ssh-keygen -t ed25519 -C "victor.perticarrari@gmail.com"
cat ~/.ssh/id_ed25519.pub
git clone git@github.com:vepo/maestro.git
sudo zypper install docker docker-compose docker-compose-switch
sudo systemctl enable docker
sudo usermod -G docker -a $USER
newgrp docker
sudo systemctl restart docker



    1  sudo systemctl restart docker
    2  git clone git@github.com:vepo/maestro.git
    3  curl -s "https://get.sdkman.io" | bash
    4  sdk list java
    5  source "/home/opensuse/.sdkman/bin/sdkman-init.sh"
    6  sdk list java
    7  sdk install java 21.0.2-open
    8  sdk install maven 3.9.9
    9  cd maestro/
   10  mvn clean package
   11  ps -aux | grep java
   12  ps -aux 
   13  cat /proc/3893/cgroup
   14  cat /sys/fs/cgroup/user.slice/user-1000.slice/session-3.scope/cpu.stat
   15  cat /sys/fs/cgroup/user.slice/memory.max
   16  ls ~/.sdkman/candidates/java/21.0.2-open/
   17  pwd ~/.sdkman/candidates/java/21.0.2-open/
   18  cat /sys/fs/cgroup/user.slice/memory.max
   19  cat /sys/fs/cgroup/system.slice/memory.max
   20  free  -m
   21  mvn clean package
   22  ./scripts/clean-stats &&  ./scripts/start-test.sh 
   23  brew install go-parquet-tools
   24  sudo zypper in go go-doc
   25  go get github.com/fraugster/parquet-go/cmd/parquet-tool
   26  go install github.com/fraugster/parquet-go/cmd/parquet-tool
   27  go install github.com/fraugster/parquet-go/cmd/parquet-tool@latest
   28  parquet-tool --help
   29  ./scripts/clean-stats &&  ./scripts/start-test.sh 
   30  rm dataset/yellow_tripdata_2025-06.*
   31  export PATH=$PATH:/home/$USER/go/bin
   32  ./scripts/clean-stats &&  ./scripts/start-test.sh 
   33  vim ~/.bashrc 
   34  echo 'export PATH=$PATH:/home/$USER/go/bin' >> ~/.bashrc
   35  source ~/.bashrc
   36  rm dataset/yellow_tripdata_2025-06.*
   37  ./scripts/clean-stats &&  ./scripts/start-test.sh 
   38  parquet-tool --help
   39  rm dataset/yellow_tripdata_2025-06.*
   40  ./scripts/clean-stats &&  ./scripts/start-test.sh 
   41  parquet-tools --help
   42  go uninstall github.com/fraugster/parquet-go/cmd/parquet-tool@latest
   43  go --help
   44  go install github.com/hangxie/parquet-tools@latest
   45  rm dataset/yellow_tripdata_2025-06.*
   46  ./scripts/clean-stats &&  ./scripts/start-test.sh 
   47  docker stats
   48  docker logs experiment-stream-maestro-1 -f
   49  ./scripts/clean-environment 
   50  history
