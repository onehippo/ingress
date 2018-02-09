package redis_client

import (
	"errors"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
	"time"
)

// Address of the redis sentinel service registered in k8s
var RedisSentinelServer = "redis:6379"

func New() *DrainedServers {

	return &DrainedServers{servers: GetServers()}
}

// ConnectRedis returns a connection to the redis sentinel
func ConnectRedis() (redis.Conn, error) {
	sc, err := redis.Dial("tcp", RedisSentinelServer,
		redis.DialConnectTimeout(5*time.Second),
		redis.DialReadTimeout(5*time.Second),
		redis.DialWriteTimeout(5*time.Second))
	if err != nil {
		glog.Warningf("Couldn't connect to redis " + RedisSentinelServer)
		return nil, err
	}
	if sc == nil {
		glog.Warningf("Couldn't connect to redis" + RedisSentinelServer)
		return nil, errors.New("Failed to connect to redis" + RedisSentinelServer)
	}
	return sc, nil
}

func GetServers() []string {
	r, err := ConnectRedis()
	if err != nil {
		glog.Warningf("Couldn't connect to redis" + RedisSentinelServer)
		return make([]string, 0)
	}
	result, _ := redis.Strings(r.Do("SMEMBERS", "servers"))
	return result
}

type DrainedServers struct {
	servers []string
}

func (c *DrainedServers) Check(address string) int {
	return Contains(c.servers, address)
}
func Contains(s []string, e string) int {
	for _, a := range s {
		if a == e {
			return 1
		}
	}
	return 0
}