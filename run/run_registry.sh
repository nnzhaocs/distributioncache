
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

docker run -p 5000:5000 -e ZOOKEEPER="hulk7:2181" -e MEMORY="100" --cpus 1 -e HOST="hulk0:5000" -v /home/nannan/dockerimages/layers:/var/lib/registry -e -e REGISTRY_STORAGE_CACHE_BLOBDESCRIPTOR=redis -e REGISTRY_REDIS_ADDR=hulk0:6379  -t nnzhaocs/distribution:latest 

#===========
docker run -d redis

sudo netstat -plnto

