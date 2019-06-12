FROM golang:latest

ENV DISTRIBUTION_DIR /go/src/github.com/docker/distribution
ENV DOCKER_BUILDTAGS include_oss include_gcs

ARG GOOS=linux
ARG GOARCH=amd64

#RUN set -ex \
#    && apk add --no-cache make git

WORKDIR $DISTRIBUTION_DIR
COPY . $DISTRIBUTION_DIR

RUN mkdir -p /go/src/bitbucket.com/milit93/ && mkdir -p ~/.ssh/ && mkdir -p /go/src/github.com/docker/docker

RUN ssh-keyscan -t rsa github.com > ~/.ssh/known_hosts

#RUN  cd /go/src/bitbucket.com/milit93/ 
RUN git clone https://github.com/nnzhaocs/consistenthash_sha256 /go/src/bitbucket.com/milit93/consistenthash_sha256/
RUN git clone https://github.com/moby/moby.git /go/src/github.com/docker/docker
#https://github.com/nnzhaocs/consistenthash_sha256

RUN apt-get update && apt-get install -y libzookeeper-mt-dev

# slight change. go-rejson has updated to v2.0 and broke compatibility. I made a clone of the old version and pointing to that version
RUN go get github.com/ngaut/log && go get github.com/allegro/bigcache && go get launchpad.net/gozk/zookeeper && go get github.com/secondspass/go-rejson && go get github.com/gomodule/redigo/redis && go get github.com/mna/redisc && go get github.com/serialx/hashring && go get github.com/go-redis/redis && go get github.com/hlts2/round-robin && go get -u github.com/panjf2000/ants

#RUN cd $DISTRIBUTION_DIR

COPY cmd/registry/config-dev.yml /etc/docker/registry/config.yml

RUN make PREFIX=/go clean binaries

VOLUME ["/var/lib/registry"]
EXPOSE 5000
ENTRYPOINT ["registry"]
CMD ["serve", "/etc/docker/registry/config.yml"]
RUN mkdir -p /docker/registry/v2/pull_tars/
RUN mkdir -p /docker/registry/v2/diskcache/
