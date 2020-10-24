package rdscache

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/easy-cache/cache"
	"github.com/gomodule/redigo/redis"
	"github.com/letsfire/redigo/v2"
	"github.com/letsfire/utils"
)

type rdsCacheDriver struct {
	client  *redigo.Client
	hashMap string
	setName string
}

func (rcd rdsCacheDriver) Get(key string) ([]byte, bool, error) {
	if rand.Intn(20) == 0 { // 5%
		go rcd.gc(time.Now().Unix())
	}
	bts, err := rcd.client.
		Bytes(func(c redis.Conn) (interface{}, error) {
			return c.Do("HGET", rcd.hashMap, key)
		})
	if err == redis.ErrNil {
		return nil, false, nil
	}
	var item cache.Item
	err = json.Unmarshal(bts, &item)
	val, ok := item.GetValue()
	if ok == false {
		_ = rcd.Del(key)
	}
	return val, ok, err
}

func (rcd rdsCacheDriver) Set(key string, val []byte, ttl time.Duration) error {
	_, err := rcd.client.
		Execute(func(c redis.Conn) (interface{}, error) {
			bts, err := json.Marshal(cache.NewItem(val, ttl))
			return nil, utils.ExecWhenNoErr(err,
				func() error {
					score := time.Now().Add(ttl).Unix()
					_, err = c.Do("ZADD", rcd.setName, score, key)
					return err
				},
				func() error {
					_, err = c.Do("HSET", rcd.hashMap, key, bts)
					return err
				},
			)
		})
	return err
}

func (rcd rdsCacheDriver) Del(key string) error {
	_, err := rcd.client.
		Execute(func(c redis.Conn) (res interface{}, err error) {
			return c.Do("HDEL", rcd.hashMap, key)
		})
	return err
}

func (rcd rdsCacheDriver) gc(max int64) error {
	_, err := rcd.client.
		Execute(func(c redis.Conn) (interface{}, error) {
			key, err := redis.Strings(c.Do("ZRANGEBYSCORE", rcd.setName, 0, max))
			return nil, utils.ExecWhenNoErr(err,
				func() error {
					args := make([]interface{}, len(key)+1)
					args[0] = rcd.hashMap
					for i := range key {
						args[i+1] = key[i]
					}
					_, err = c.Do("HDEL", args...)
					return err
				},
				func() error {
					_, err = c.Do("ZREMRANGEBYSCORE", rcd.setName, 0, max)
					return err
				},
			)
		})
	return err
}

func NewDriver(hashMap string, client *redigo.Client) cache.DriverInterface {
	return rdsCacheDriver{hashMap: hashMap, client: client, setName: fmt.Sprintf("%s-zset", hashMap)}
}

func NewCache(hashMap string, client *redigo.Client, args ...interface{}) cache.Interface {
	return cache.New(append(args, NewDriver(hashMap, client))...)
}
