# broker
    
    go broker interface,you can use kafka,redis,pulsar etc.

# pulsar in docker

    run pulsar in docker
    docker run -dit \
    --name pulsar-sever \
    -p 6650:6650 \
    -p 8080:8080 \
    --mount source=pulsardata,target=/pulsar/data \
    --mount source=pulsarconf,target=/pulsar/conf \
    apachepulsar/pulsar:2.7.4 \
    bin/pulsar standalone

# pulsar-go
https://pulsar.apache.org/docs/zh-CN/client-libraries-go/

# usage

    For specific usage, refer to gpulsar/gredis test
    kafka consumer groups require Version to be >= V0_10_2_0
    if lower than V0_10_2_0, please use go-god/broker v1.1.0
