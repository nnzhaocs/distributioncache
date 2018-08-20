
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
