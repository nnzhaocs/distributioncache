
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

#docker run -p 5000:5000 -e ZOOKEEPER="hulk7:2181" -e MEMORY="100" --cpus 1 -e HOST="hulk0:5000" -v /home/nannan/dockerimages/layers:/var/lib/registry -e REGISTRY_STORAGE_CACHE_BLOBDESCRIPTOR=redis -e REGISTRY_REDIS_ADDR=192.168.0.170:6379  -t nnzhaocs/distribution:latest

docker run -p 5000:5000 -e MEMORY="100" --cpus 1 -e HOST="hulk0:5000" -v /home/nannan/dockerimages/layers:/var/lib/registry  -t nnzhaocs/distribution:latest

docker run -p 5000:5000 -v /home/lustre/dockerimages/layers:/var/lib/registry -e REGISTRY_REDIS_ADDR=192.168.0.209:6379 -e REGISTRY_STORAGE_CACHE_HOSTIP=192.168.0.203 --name nnregistry -t nnzhaocs/distribution:latest

#=========================> HOW TO RUN REDIS WITH REGISTRY <=====================
#docker run -d -p 6379:6379 redis

docker run -p 6379:6379 --name redis-rejson redislabs/rejson:latest

goto nitishm/go-rejson
./install-redis-rejson.sh
./start-redis-rejson.sh

go get github.com/gomodule/redigo/redis
go get github.com/nitishm/go-rejson

sudo netstat -plnto

