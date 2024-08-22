package conn

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-session/session/v3"
)

func (rc *RedisConf) NewManagerStore(ctx context.Context, name string) (session.ManagerStore, error) {
	db, ok := rc.DbMap[name]
	if !ok {
		return nil, errors.New("db not found: " + name)
	}
	clt := redis.NewClient(&redis.Options{
		Addr: rc.Host,
		DB:   db,
	})
	if clt.Ping(ctx).Val() != "PONG" {
		return nil, errors.New("redis connect error")
	}
	return &managerStore{clt: clt, prefix: "session_" + name}, nil
}

type managerStore struct {
	clt    *redis.Client
	prefix string
}

func (m *managerStore) getKey(key string) string {
	return m.prefix + key
}

// Check the session store exists
func (m *managerStore) Check(ctx context.Context, sid string) (bool, error) {
	cmd := m.clt.Exists(ctx, m.getKey(sid))
	if err := cmd.Err(); err != nil {
		return false, err
	}
	return cmd.Val() > 0, nil
}

// Create a session store and specify the expiration time (in seconds)
func (m *managerStore) Create(ctx context.Context, sid string, expired int64) (session.Store, error) {
	return newStore(ctx, m, sid, expired, nil), nil
}

// Update a session store and specify the expiration time (in seconds)
func (m *managerStore) Update(ctx context.Context, sid string, expired int64) (session.Store, error) {
	value, err := m.getValue(ctx, sid)
	if err != nil {
		return nil, err
	} else if value == nil {
		return newStore(ctx, m, sid, expired, nil), nil
	}

	cmd := m.clt.Expire(ctx, m.getKey(sid), time.Duration(expired)*time.Second)
	if err = cmd.Err(); err != nil {
		return nil, err
	}

	values, err := m.parseValue(value)
	if err != nil {
		return nil, err
	}

	return newStore(ctx, m, sid, expired, values), nil
}

// Delete a session store
func (m *managerStore) Delete(ctx context.Context, sid string) error {
	if ok, err := m.Check(ctx, sid); err != nil {
		return err
	} else if !ok {
		return nil
	}

	cmd := m.clt.Del(ctx, m.getKey(sid))
	return cmd.Err()
}

// Use sid to replace old sid and return session store
func (m *managerStore) Refresh(ctx context.Context, oldsid, sid string, expired int64) (session.Store, error) {
	value, err := m.getValue(ctx, oldsid)
	if err != nil {
		return nil, err
	} else if value == nil {
		return newStore(ctx, m, sid, expired, nil), nil
	}

	pipe := m.clt.TxPipeline()
	pipe.Set(ctx, m.getKey(sid), value, time.Duration(expired)*time.Second)
	pipe.Del(ctx, m.getKey(oldsid))
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	values, err := m.parseValue(value)
	if err != nil {
		return nil, err
	}

	return newStore(ctx, m, sid, expired, values), nil
}

func (m *managerStore) getValue(ctx context.Context, sid string) ([]byte, error) {
	cmd := m.clt.Get(ctx, m.getKey(sid))
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	return cmd.Bytes()
}

func (s *managerStore) parseValue(bytes []byte) (map[string]interface{}, error) {
	var values map[string]interface{}
	if len(bytes) > 0 {
		err := json.Unmarshal(bytes, &values)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

// Close storage, release resources
func (m *managerStore) Close() error {
	return m.clt.Close()
}

func newStore(ctx context.Context, ms *managerStore, sid string, expired int64, values map[string]interface{}) *store {
	if values == nil {
		values = make(map[string]interface{})
	}

	return &store{
		ms:      ms,
		ctx:     ctx,
		sid:     sid,
		expired: expired,
		values:  values,
	}
}

type store struct {
	sync.RWMutex
	ms      *managerStore
	ctx     context.Context
	sid     string
	expired int64
	values  map[string]interface{}
}

func (s *store) Context() context.Context {
	return s.ctx
}

func (s *store) SessionID() string {
	return s.sid
}

func (s *store) Set(key string, value interface{}) {
	s.Lock()
	s.values[key] = value
	s.Unlock()
}

func (s *store) Get(key string) (interface{}, bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok := s.values[key]
	return val, ok
}

func (s *store) Delete(key string) interface{} {
	s.RLock()
	v, ok := s.values[key]
	s.RUnlock()
	if ok {
		s.Lock()
		delete(s.values, key)
		s.Unlock()
	}
	return v
}

func (s *store) Flush() error {
	s.Lock()
	s.values = make(map[string]interface{})
	s.Unlock()
	return s.Save()
}

func (s *store) Save() error {
	var buf []byte
	var err error
	s.RLock()
	if len(s.values) > 0 {
		buf, err = json.Marshal(s.values)
		if err != nil {
			s.RUnlock()
			return err
		}
	}
	s.RUnlock()

	cmd := s.ms.clt.Set(s.ctx, s.ms.getKey(s.sid), buf, time.Duration(s.expired)*time.Second)
	return cmd.Err()
}
