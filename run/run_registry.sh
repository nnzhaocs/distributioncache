
#! THIS IS AN EXAMPLE SHOWING HOW TO IMPORT CODE TO CONTAINER AND BUILD AS AN IMAGE
#
docker run -ti -v /home/nannan/go/src/github.com/docker:/docker nnzhaocs/distribution:golang--tag
#
docker commit --change "ENV DEBUG true" 179177d83d0d  nnzhaocs/distribution:src--tag
#
docker push nnzhaocs/distribution:src--tag
#
docker cp 0ca7ddb4282d:/go/src/github.com/docker/distribution/bin/registry ./
#
docker build -t nnzhaocs/distribution:registry .
#
docker stop container_id

#==========================>

docker login

docker push nnzhaocs/distribution:latest
docker tag nnzhaocs/distribution:latest nnzhaocs/socc-sfit-dedup

####:==========cleanup for registry =============
pssh -h remotehosts.txt -l root -A 'docker stop $(docker ps -a -q)'
pssh -h remotehosts.txt -l root -A 'rm -rf /home/nannan/testing/tmpfs/*'
pssh -h remotehosts.txt -l root -A 'rm -rf /home/nannan/testing/layers/*'

pssh -h remotehostthors.txt -l root -A -i 'mount -t tmpfs -o size=8G tmpfs /home/nannan/testing/tmpfs'

./flushall-cluster.sh 192.168.0.170


####:==========run siftregistry ==================
sudo docker run -p 5000:5000 -d --rm --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v /home/nannan/testing/layers:/var/lib/registry -e "REGISTRY_STORAGE_CACHE_HOSTIP=$(ip -4 addr |grep 192.168 |grep -Po 'inet \K[\d.]+')" --name dedup-test -t nnzhaocs/distribution:latest

####:============run traditionaldedupregistrycluster======================######
#sudo docker service create --name traditionaldedupregistry --replicas 10 --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v /home/nannan/testing/layers:/var/lib/registry -e "REGISTRY_STORAGE_CACHE_HOSTIP=$(ip -4 addr |grep 192.168 |grep -Po 'inet \K[\d.]+')"

#####: For thors
#pssh -h remotehosts.txt -l root -A -i 'docker run --rm -d -p 5000:5000 --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v=/home/nannan/testing/layers:/var/lib/registry -e "REGISTRY_STORAGE_CACHE_HOSTIP=$(ip -4 addr |grep 192.168.0.2 |grep -Po "inet \K[\d.]+")" --name traditionaldedup-3  nnzhaocs/distribution:traditionaldedup'
#pssh -h remotehostthors.txt -l root -A -i 'docker run --rm -d -p 5000:5000 --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v=/home/nannan/testing/layers:/var/lib/registry -e "REGISTRY_STORAGE_CACHE_HOSTIP=$(ip -4 addr |grep 192.168.0.2 |grep -Po "inet \K[\d.]+")" --name traditionaldedup-3 --net=host nnzhaocs/distribution:traditionaldedup'

#### set up amaranth registries #######
#pssh -h remotehostthors.txt -l root -A -i 'docker run --rm -d -p 5000:5000 -v=/home/nannan/testing/layers:/var/lib/registry --name random-registry-cluster registry'

1. cleanup hulks same as before, and cleanup amaranths as: 
pssh -h remotehotamaranths.txt -l root -A 'docker stop $(docker ps -a -q)' 
pssh -h remotehotamaranths.txt -l root -A 'rm -rf /home/nannan/testing/layers/*'

2. setup amaranth registries first:
pssh -h remotehotamaranths.txt -l root -A -i 'docker run --rm -d -p 5000:5000 -v=/home/nannan/testing/layers:/var/lib/registry --name random-registry-cluster registry'

3. setup hulk registries:
pssh -h remotehosts.txt -l root -A -i 'docker run --rm -d -p 5000:5000 --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v=/home/nannan/testing/layers:/var/lib/registry -e "REGISTRY_STORAGE_CACHE_HOSTIP=$(ip -4 addr |grep 192.168 |grep -Po "inet \K[\d.]+")" --name traditionaldedupregistry-3  nnzhaocs/distribution:traditionaldedup'

4. run docker-performance:
config_1.yaml configuration: 
traditionaldedup: True; others are set to false; warmup threads: 10;
Others same as before.


5. save parameters:
Two kinds of values: ones start with "Blob:File:Recipe::sha256" and ones start with "Blob:File:Recipe::RestoreTime::sha256"

For the ones with "Blob:File:Recipe::RestoreTime::sha256*"
we need to save:
BlobDigest:
UncompressSize:
CompressSize:

For the ones with "Blob:File:Recipe::sha256*"
we need to save
key
SliceSize
DurationCP
DurationCMP
DurationML
DurationNTT
DurationRS

So inaddition to value fields, we need to save the key as well for "Blob:File:Recipe::sha256*"

####:============run originalregistrycluster======================######

#pssh -h remotehosts.txt -l root -A 'docker run --rm -d -p 5000:5000 --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v=/home/nannan/testing/layers:/var/lib/registry -e "REGISTRY_STORAGE_CACHE_HOSTIP=$(ip -4 addr |grep 192.168 |grep -Po 'inet \K[\d.]+')" --name originalregistry  nnzhaocs/distribution:original'

pssh -h remotehosts.txt -l root -A -i 'docker run --rm -d -p 5000:5000 --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v=/home/nannan/testing/layers:/var/lib/registry -e "REGISTRY_STORAGE_CACHE_HOSTIP=$(ip -4 addr |grep 192.168 |grep -Po "inet \K[\d.]+")" --name originalregistry-3  nnzhaocs/distribution:original'

#sudo docker service create -p 5000:5000 --replicas=9 --mount type=tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ --mount type=bind,source=/home/nannan/testing/layers,target=/var/lib/registry -e "REGISTRY_STORAGE_CACHE_HOSTIP=$(ip -4 addr |grep 192.168 |grep -Po 'inet \K[\d.]+')" --name originalregistry  nnzhaocs/distribution:original

####: ============run randomregistrycluster on amaranths============#####
sudo docker service create --name randomregistry --replicas 5 --mount type=bind,source=/home/nannan/testing/layers,destination=/var/lib/registry -p 5000:5000 registry


#docker run -p 5001:5000 -e ZOOKEEPER="hulk7:2181" -e MEMORY="100" --cpus 1 -e HOST="hulk0:5000" -v /home/nannan/dockerimages/layers:/var/lib/registry -e REGISTRY_STORAGE_CACHE_BLOBDESCRIPTOR=redis -e REGISTRY_REDIS_ADDR=192.168.0.170:6379  -t nnzhaocs/distribution:latest

#docker run -p 5000:5000 -e MEMORY="100" --cpus 1 -e HOST="hulk0:5000" -v /home/nannan/dockerimages/layers:/var/lib/registry  -t nnzhaocs/distribution:latest

#docker run -l error --config ~/testing/layers/config-dev.yaml -p 5000:5000 --rm -v /home/lustre/dockerimages/layers:/var/lib/registry -e REGISTRY_STORAGE_CACHE_HOSTIP=192.168.0.220 --name nnregistry -t nnzhaocs/distribution:latest
#=======================> HOW TO BUILD AND RUN REGISTRY <====================
docker build -t nnzhaocs/socc-sift-dedup ./

docker push nnzhaocs/socc-sfit-dedup

sudo docker run -p 5000:5000 --rm --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v /home/nannan/testing/layers:/var/lib/registry -e REGISTRY_STORAGE_CACHE_HOSTIP=192.168.0.171 -e REGISTRY_NOTIFICATIONS_REGISTRIES=192.168.0.170,192.168.0.171  --name dedup-9cluster -t nnzhaocs/distribution:latest




#========================> HOW TO CREATE A REDIS CLUSTER WITH DOCKER SWARM ,source=/home/nannan/testing<===================

sudo docker service create --name rejson-cluster -p 6379:6379 --replicas=9 redislabs/rejson \
    --cluster-enabled yes\
            --cluster-node-timeout 60000

#========================> STALE HOW TO RUN A REDIS CLUSTER WITH DOCKER <=============
$PWD is distribution/run dir

#create a network
#docker network create redis_cluster
#create redis containers
(1) start redis instance
docker run -p 7000:7000 -d -v $PWD/redis-cluster-7000.conf:/usr/local/etc/redis/redis.conf -v $PWD/rejson.so:/home/rejson.so --name redis-0 --net host redis redis-server /usr/local/etc/redis/redis.conf
docker run -p 7001:7001 -d -v $PWD/redis-cluster-7001.conf:/usr/local/etc/redis/redis.conf -v $PWD/rejson.so:/home/rejson.so --name redis-1 --net host  redis redis-server /usr/local/etc/redis/redis.conf
docker run -p 7002:7002 -d -v $PWD/redis-cluster-7002.conf:/usr/local/etc/redis/redis.conf -v $PWD/rejson.so:/home/rejson.so --name redis-2 --net host redis redis-server /usr/local/etc/redis/redis.conf
docker run -p 7003:7003 -d -v $PWD/redis-cluster-7003.conf:/usr/local/etc/redis/redis.conf -v $PWD/rejson.so:/home/rejson.so --name redis-3 --net host redis redis-server /usr/local/etc/redis/redis.conf
docker run -p 7004:7004 -d -v $PWD/redis-cluster-7004.conf:/usr/local/etc/redis/redis.conf -v $PWD/rejson.so:/home/rejson.so --name redis-4 --net host redis redis-server /usr/local/etc/redis/redis.conf
docker run -p 7005:7005 -d -v $PWD/redis-cluster-7005.conf:/usr/local/etc/redis/redis.conf -v $PWD/rejson.so:/home/rejson.so --name redis-5 --net host redis redis-server /usr/local/etc/redis/redis.conf

(2) create a cluster
docker exec -ti redis-0 redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 --cluster-replicas 1
docker exec -ti rejson-cluster redis-cli --cluster create 192.168.0.170:6379 192.168.0.171:6379 192.168.0.172:6379 192.168.0.174:6379 192.168.0.176:6379 192.168.0.177:6379 192.168.0.178:6379 192.168.0.179:6379 192.168.0.180:6379 --cluster-replicas 1

(3) check?
docker exec -ti  redis-rejson-2 redis-cli

(4) and check and init cluster
go run ../test/redis_cluster.go

#=========================> HOW TO RUN REDIS WITH REGISTRY <=====================
#docker run -d -p 6379:6379 redis
docker run -p 6379:6379 --name redis-rejson redislabs/rejson:latest
redis-cli FLUSHALL

goto nitishm/go-rejson
./install-redis-rejson.sh
./start-redis-rejson.sh

go get github.com/gomodule/redigo/redis
go get github.com/nitishm/go-rejson

sudo netstat -plnto

