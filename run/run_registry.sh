
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

#docker run -p 5001:5000 -e ZOOKEEPER="hulk7:2181" -e MEMORY="100" --cpus 1 -e HOST="hulk0:5000" -v /home/nannan/dockerimages/layers:/var/lib/registry -e REGISTRY_STORAGE_CACHE_BLOBDESCRIPTOR=redis -e REGISTRY_REDIS_ADDR=192.168.0.170:6379  -t nnzhaocs/distribution:latest

#docker run -p 5000:5000 -e MEMORY="100" --cpus 1 -e HOST="hulk0:5000" -v /home/nannan/dockerimages/layers:/var/lib/registry  -t nnzhaocs/distribution:latest

#docker run -l error --config ~/testing/layers/config-dev.yaml -p 5000:5000 --rm -v /home/lustre/dockerimages/layers:/var/lib/registry -e REGISTRY_STORAGE_CACHE_HOSTIP=192.168.0.220 --name nnregistry -t nnzhaocs/distribution:latest
#=======================> HOW TO BUILD AND RUN REGISTRY <====================
docker build -t nnzhaocs/socc-sift-dedup ./

docker push nnzhaocs/socc-sfit-dedup

sudo docker run -p 5000:5000 --rm --mount type=bind,source=/home/nannan/testing/tmpfs,target=/var/lib/registry/docker/registry/v2/pull_tars/ -v /home/nannan/testing/layers:/var/lib/registry -e REGISTRY_STORAGE_CACHE_HOSTIP=192.168.0.171 -e REGISTRY_NOTIFICATIONS_REGISTRIES=192.168.0.170,192.168.0.171  --name dedup-9cluster -t nnzhaocs/distribution:latest

#========================> HOW TO CREATE A REDIS CLUSTER WITH DOCKER SWARM <===================

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

