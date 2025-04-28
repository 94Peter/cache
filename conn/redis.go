package conn

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-session/session/v3"
)

type RedisDI interface {
	NewRedisDbConn(name string) (RedisClient, error)
	NewManagerStore(ctx context.Context, name string) (session.ManagerStore, error)
}

type RedisConf struct {
	Host  string         `yaml:"host"`
	Pwd   string         `yaml:"pass"`
	DbMap map[string]int `yaml:"dbMap"`

	sync.Mutex
	connMap map[string]*redis.Client
}

func (rc *RedisConf) NewRedisDbConn(name string) (RedisClient, error) {
	rc.Lock()
	defer rc.Unlock()
	if rc.connMap == nil {
		rc.connMap = make(map[string]*redis.Client)
	}
	var redisclt *redis.Client
	var ok bool
	db, ok := rc.DbMap[name]
	if !ok {
		return nil, errors.New("db not found: " + name)
	}
	if redisclt, ok = rc.connMap[name]; !ok {
		redisclt = redis.NewClient(&redis.Options{
			Addr: rc.Host,
			// Password:     r.Pwd, // no password set
			DB:           db, // use default DB
			PoolSize:     10,
			MinIdleConns: 3,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolTimeout:  4 * time.Second,
			IdleTimeout:  5 * time.Minute,
		})
		rc.connMap[name] = redisclt
	}

	r := &redisV8CltImpl{
		clt:   redisclt,
		retry: 3,
		db:    db,
	}

	if r.Ping(context.Background()) != "PONG" {
		return nil, errors.New("redis connect error")
	}
	return r, nil
}

type RedisClient interface {
	Close() error
	Ping(ctx context.Context) string
	CountKeys(ctx context.Context) (int, error)
	Get(ctx context.Context, k string) ([]byte, error)
	Set(ctx context.Context, k string, v interface{}, exp time.Duration) (string, error)
	Del(ctx context.Context, k ...string) (int64, error)
	DelKeys(ctx context.Context, pattern string) (int64, error)
	LPush(ctx context.Context, k string, v interface{}) (int64, error)
	RPop(ctx context.Context, k string) ([]byte, error)
	HGet(ctx context.Context, key string, field string) string
	HSet(ctx context.Context, key string, values map[string]string) error
	HGetAll(ctx context.Context, key string) map[string]string
	Exists(ctx context.Context, key string) bool
	Expired(ctx context.Context, key string, d time.Duration) (bool, error)
	NewPiple(ctx context.Context) CachePipel
	Keys(ctx context.Context, pattern string) ([]string, error)
	TTL(ctx context.Context, key string) (time.Duration, error)
	MGet(ctx context.Context, keys []string) ([]interface{}, error)
}

type CachePipel interface {
	Get(key string) *redis.StringCmd
	Exec() ([]redis.Cmder, error)
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
}

type redisV8CltImpl struct {
	clt   *redis.Client
	retry int
	db    int
}

func (rci *redisV8CltImpl) Keys(ctx context.Context, pattern string) ([]string, error) {
	var err error
	var keys []string
	for i := 0; i < rci.retry; i++ {
		keys, err = rci.clt.Keys(ctx, pattern).Result()
		if err == nil {
			return keys, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return nil, errors.New("redis get keys error: " + err.Error())
}

func (rci *redisV8CltImpl) Get(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.Get(ctx, key).Bytes()
		if err == nil {
			return result, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return nil, errors.New("redis get error: " + err.Error())
}

func (rci *redisV8CltImpl) Expired(ctx context.Context, key string, d time.Duration) (bool, error) {
	var result bool
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.Expire(ctx, key, d).Result()
		if err == nil {
			return result, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return false, errors.New("redis expired error: " + err.Error())
}

func (rci *redisV8CltImpl) CountKeys(ctx context.Context) (int, error) {
	var r string
	var err error
	for i := 0; i < rci.retry; i++ {
		r, err = rci.clt.Info(ctx, "keyspace").Result()
		if err == nil {
			break
		}
		time.Sleep(time.Microsecond * 200)
	}
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
	if rci.clt == nil {
		return nil
	}
	return rci.clt.Close()
}

func (rci *redisV8CltImpl) Ping(ctx context.Context) string {
	return rci.clt.Ping(ctx).Val()
}

func (rci *redisV8CltImpl) Set(ctx context.Context, k string, v interface{}, exp time.Duration) (string, error) {
	var result string
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.Set(ctx, k, v, exp).Result()
		if err == nil {
			return result, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return "", errors.New("redis set error: " + err.Error())
}

func (rci *redisV8CltImpl) Del(ctx context.Context, k ...string) (int64, error) {
	var result int64
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.Del(ctx, k...).Result()
		if err == nil {
			return result, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return 0, errors.New("redis del error: " + err.Error())
}

func (rci *redisV8CltImpl) DelKeys(ctx context.Context, pattern string) (int64, error) {
	var keys []string
	var err error
	for i := 0; i < rci.retry; i++ {
		keys, err = rci.clt.Keys(ctx, pattern).Result()
		if err == nil {
			break
		}
		time.Sleep(time.Microsecond * 200)
	}
	if len(keys) == 0 {
		return 0, nil
	}
	return rci.clt.Del(ctx, keys...).Result()
}

func (rci *redisV8CltImpl) LPush(ctx context.Context, k string, v interface{}) (int64, error) {
	var result int64
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.LPush(ctx, k, v).Result()
		if err == nil {
			return result, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return 0, errors.New("redis lpush error: " + err.Error())
}

func (rci *redisV8CltImpl) RPop(ctx context.Context, k string) ([]byte, error) {
	var result []byte
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.RPop(ctx, k).Bytes()
		if err == nil {
			return result, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return nil, errors.New("redis rpop error: " + err.Error())
}

func (rci *redisV8CltImpl) Exists(ctx context.Context, key string) bool {
	var result int64
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.Exists(ctx, key).Result()
		if err == nil {
			return result == 1
		}
		time.Sleep(time.Microsecond * 200)
	}
	return false
}

func (rci *redisV8CltImpl) HGetAll(ctx context.Context, key string) map[string]string {
	var result map[string]string
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.HGetAll(ctx, key).Result()
		if err == nil {
			return result
		}
		time.Sleep(time.Microsecond * 200)
	}
	return map[string]string{}
}

func (rci *redisV8CltImpl) HGet(ctx context.Context, key string, field string) string {
	var result string
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.HGet(ctx, key, field).Result()
		if err == nil {
			return result
		}
		time.Sleep(time.Microsecond * 200)
	}
	return ""
}

func (rci *redisV8CltImpl) HSet(ctx context.Context, key string, values map[string]string) error {
	var err error
	vals := make([]any, len(values)*2)
	i := 0
	for k, v := range values {
		vals[i] = k
		vals[i+1] = v
		i += 2
	}
	for i := 0; i < rci.retry; i++ {
		err = rci.clt.HSet(ctx, key, vals...).Err()
		if err == nil {
			return nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return err
}

func (rci *redisV8CltImpl) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	var result []any
	var err error
	for i := 0; i < rci.retry; i++ {
		result, err = rci.clt.MGet(ctx, keys...).Result()
		if err == nil {
			return result, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return nil, errors.New("redis mget error: " + err.Error())
}

func (p *redisV8CltImpl) TTL(ctx context.Context, key string) (time.Duration, error) {
	var result time.Duration
	var err error
	for i := 0; i < p.retry; i++ {
		result, err = p.clt.TTL(ctx, key).Result()
		if err == nil {
			return result, nil
		}
		time.Sleep(time.Microsecond * 200)
	}
	return 0, errors.New("redis ttl error: " + err.Error())
}

func (rci *redisV8CltImpl) NewPiple(ctx context.Context) CachePipel {
	return &myPipel{
		redisPiple: rci.clt.Pipeline(),
		ctx:        ctx,
	}
}

type myPipel struct {
	redisPiple redis.Pipeliner
	ctx        context.Context
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
