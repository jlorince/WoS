#!/bin/bash


### DATA LOCATION: s3://klab-jobs/inputs/jlorince/wos-features-normed-w2v-200.txt

# get rootdir
d=$(pwd)

### Setup scratch space
export DEBIAN_FRONTEND=noninteractive
lsblk
apt-get install -y mdadm
if [[ ! -d '/scratch' ]]
then
     pushd .
     mkdir /scratch
     disks=($(lsblk | grep 'xvd' | grep -v 'xvda' | awk '{print $1}'))
     echo "Attempting to mount disks : ${disks[*]}" ;
     cd /dev;  mdadm --create --verbose /dev/md0 --level=stripe --raid-devices=${#disks[*]} ${disks[*]}
     mkfs.ext4 /dev/md0; mount -t ext4 /dev/md0 /scratch; chmod 777 /scratch                                                                                                                                        
     popd 
fi

mv wos-features-normed-w2v-200.txt /scratch/
cd /scratch

### Setup and installation for LargeVis
sudo apt-get install libgsl0-dev
git clone https://github.com/lferry007/LargeVis.git
cd LargeVis/Linux/
g++ LargeVis.cpp main.cpp -o LargeVis -lm -pthread -lgsl -lgslcblas -Ofast -march=native -ffast-math
sudo  python setup.py install

cd ..

python LargeVis_run.py -input ../wos-features-normed-w2v-200.txt -output ../wos-w2v-200-lv-output.txt -threads 32

mv ../wos-w2v-200-lv-output.txt $d/
