package conn

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-session/session/v3"
)

type RedisDI interface {
	NewRedisDbConn(ctx context.Context, name string) (RedisClient, error)
	NewManagerStore(ctx context.Context, name string) (session.ManagerStore, error)
}

type RedisConf struct {
	Host  string         `yaml:"host"`
	Pwd   string         `yaml:"pass"`
	DbMap map[string]int `yaml:"dbMap"`
}

func (rc *RedisConf) NewRedisDbConn(ctx context.Context, name string) (RedisClient, error) {
	//const connTimeout = time.Second * 5
	db, ok := rc.DbMap[name]
	if !ok {
		return nil, errors.New("db not found: " + name)
	}
	r := &redisV8CltImpl{
		clt: redis.NewClient(&redis.Options{
			Addr: rc.Host,
			// Password:     r.Pwd, // no password set
			DB: db, // use default DB
		}),
		ctx: ctx,
		db:  db,
	}

	if r.Ping() != "PONG" {
		return nil, errors.New("redis connect error")
	}
	return r, nil
}

type RedisClient interface {
	Close() error
	Ping() string
	CountKeys() (int, error)
	Get(k string) ([]byte, error)
	Set(k string, v interface{}, exp time.Duration) (string, error)
	Del(k ...string) (int64, error)
	DelKeys(pattern string) (int64, error)
	LPush(k string, v interface{}) (int64, error)
	RPop(k string) ([]byte, error)
	HGet(key string, field string) string
	HSet(key string, values map[string]string) error
	HGetAll(key string) map[string]string
	Exists(key string) bool
	Expired(key string, d time.Duration) (bool, error)
	NewPiple() CachePipel
	Keys(pattern string) ([]string, error)
	TTL(key string) (time.Duration, error)
	MGet(keys []string) ([]interface{}, error)
}

type CachePipel interface {
	Get(key string) *redis.StringCmd
	Exec() ([]redis.Cmder, error)
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
}

type redisV8CltImpl struct {
	clt *redis.Client
	ctx context.Context
	db  int
}

func (rci *redisV8CltImpl) Keys(pattern string) ([]string, error) {
	return rci.clt.Keys(rci.ctx, pattern).Result()
}

func (rci *redisV8CltImpl) Get(key string) ([]byte, error) {
	return rci.clt.Get(rci.ctx, key).Bytes()
}

func (rci *redisV8CltImpl) Expired(key string, d time.Duration) (bool, error) {
	return rci.clt.Expire(rci.ctx, key, d).Result()
}

func (rci *redisV8CltImpl) CountKeys() (int, error) {
	r := rci.clt.Info(rci.ctx, "keyspace").String()
	k := fmt.Sprintf("db%d:keys=", rci.db)
	i := strings.Index(r, k)
	l := len(r)
	var count []byte
	for i = i + len(k); i < l; i++ {
		if r[i] == ',' {
			break
		}
		count = append(count, r[i])
	}
	return strconv.Atoi(string(count))
}

func (rci *redisV8CltImpl) Close() error {
	return rci.clt.Close()
}

func (rci *redisV8CltImpl) Ping() string {
	return rci.clt.Ping(rci.ctx).Val()
}

func (rci *redisV8CltImpl) Set(k string, v interface{}, exp time.Duration) (string, error) {
	return rci.clt.Set(rci.ctx, k, v, exp).Result()
}

func (rci *redisV8CltImpl) Del(k ...string) (int64, error) {
	return rci.clt.Del(rci.ctx, k...).Result()
}

func (rci *redisV8CltImpl) DelKeys(pattern string) (int64, error) {
	keys, err := rci.Keys(pattern)
	if err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}
	return rci.clt.Del(rci.ctx, keys...).Result()
}

func (rci *redisV8CltImpl) LPush(k string, v interface{}) (int64, error) {
	return rci.clt.LPush(rci.ctx, k, v).Result()
}

func (rci *redisV8CltImpl) RPop(k string) ([]byte, error) {
	return rci.clt.RPop(rci.ctx, k).Bytes()
}

func (rci *redisV8CltImpl) Exists(key string) bool {
	return rci.clt.Exists(rci.ctx, key).Val() == 1
}

func (rci *redisV8CltImpl) HGetAll(key string) map[string]string {
	return rci.clt.HGetAll(rci.ctx, key).Val()
}

func (rci *redisV8CltImpl) HGet(key string, field string) string {
	return rci.clt.HGet(rci.ctx, key, field).Val()
}

func (rci *redisV8CltImpl) HSet(key string, values map[string]string) error {
	vals := make([]any, len(values)*2)
	i := 0
	for k, v := range values {
		vals[i] = k
		vals[i+1] = v
		i += 2
	}
	return rci.clt.HSet(rci.ctx, key, vals...).Err()
}

func (rci *redisV8CltImpl) MGet(keys []string) ([]interface{}, error) {
	return rci.clt.MGet(rci.ctx, keys...).Result()
}

func (p *redisV8CltImpl) TTL(key string) (time.Duration, error) {
	ttlcmd := p.clt.TTL(p.ctx, key)
	if err := ttlcmd.Err(); err != nil {
		return 0, err
	}
	return ttlcmd.Val(), nil
}

func (rci *redisV8CltImpl) NewPiple() CachePipel {
	return &myPipel{
		redisPiple: rci.clt.Pipeline(),
		ctx:        rci.ctx,
	}
}

type myPipel struct {
	ctx        context.Context
	redisPiple redis.Pipeliner
}

func (p *myPipel) Get(key string) *redis.StringCmd {
	return p.redisPiple.Get(p.ctx, key)
}

func (p *myPipel) Exec() ([]redis.Cmder, error) {
	return p.redisPiple.Exec(p.ctx)
}

func (p *myPipel) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return p.redisPiple.Set(p.ctx, key, value, expiration)
}
