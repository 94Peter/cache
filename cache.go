package cache

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/94peter/cache/conn"
)

type Cache interface {
	SaveObjs(exp time.Duration, docs ...CacheObj) error
	SaveObj(i CacheObj, exp time.Duration) error
	GetObj(key string, i CacheObj) error
	GetObjs(keys []string, d CacheObj) (objs []CacheObj, err error)
	SaveObjHash(i CacheMapObj, exp time.Duration) error
	GetObjHash(key string, i CacheMapObj) error
}

func NewRedisCache(clt conn.RedisClient) Cache {
	return &redisCache{
		RedisClient: clt,
	}
}

type redisCache struct {
	conn.RedisClient
}

func (c *redisCache) SaveObjs(exp time.Duration, docs ...CacheObj) error {
	pipe := c.NewPiple()
	for _, d := range docs {
		b, err := d.Encode()
		if err != nil {
			return err
		}
		pipe.Set(d.GetKey(), b, exp)
	}
	_, err := pipe.Exec()
	return err
}

func (c *redisCache) SaveObj(i CacheObj, exp time.Duration) error {

	b, err := i.Encode()
	if err != nil {
		return err
	}
	_, err = c.Set(i.GetKey(), b, exp)
	return err
}

func (c *redisCache) GetObj(key string, i CacheObj) error {
	if reflect.ValueOf(i).Type().Kind() != reflect.Ptr {
		return errors.New("must be pointer")
	}
	data, err := c.Get(key)
	if err != nil {
		return err
	}

	expired, err := c.TTL(key)
	if err != nil {
		return err
	}
	i.SetExpiredTime(expired)
	err = i.Decode(data)
	return err
}

func (c *redisCache) GetObjs(keys []string, d CacheObj) (objs []CacheObj, err error) {
	var sliceList []CacheObj

	objType := reflect.TypeOf(d)
	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
	}

	var newValue reflect.Value
	var newDoc CacheObj
	pipe := c.NewPiple()
	for _, k := range keys {
		newValue = reflect.New(objType)
		newDoc = newValue.Interface().(CacheObj)
		newDoc.SetStringCmd(pipe.Get(k))
		sliceList = append(sliceList, newDoc)
		expired, err := c.TTL(k)
		if err == nil {
			newDoc.SetExpiredTime(expired)
		}
	}
	pipe.Exec()
	for _, s := range sliceList {
		if !s.HasError() {
			s.DecodePipe()
		}
	}
	return sliceList, nil
}

func (c *redisCache) SaveObjHash(i CacheMapObj, exp time.Duration) error {
	data, err := i.EncodeMap()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	err = c.RedisClient.HSet(i.GetKey(), data)
	if err != nil {
		return fmt.Errorf("set hash error: %w", err)
	}
	if exp <= 0 {
		return nil
	}
	_, err = c.RedisClient.Expired(i.GetKey(), exp)
	if err != nil {
		return fmt.Errorf("set expired fail: %w", err)
	}
	return nil
}

func (c *redisCache) GetObjHash(key string, i CacheMapObj) error {
	expired, err := c.RedisClient.TTL(key)
	if err != nil {
		return err
	}
	i.SetExpiredTime(expired)
	data := c.RedisClient.HGetAll(key)
	return i.DecodeMap(data)
}
