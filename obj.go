package cache

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/go-redis/redis/v8"
)

type CacheObj interface {
	GetKey() string
	Encode() ([]byte, error)
	SetStringCmd(sc *redis.StringCmd)
	Decode([]byte) error
	DecodePipe() error
	GetError() error
	HasError() bool
}

type ComCacheObj struct {
	scmd *redis.StringCmd
}

func (s *ComCacheObj) SetStringCmd(sc *redis.StringCmd) {
	s.scmd = sc
}

func (s *ComCacheObj) GetStringCmd() (*redis.StringCmd, error) {
	if s.scmd == nil {
		return nil, errors.New("scmd not set")
	}
	return s.scmd, nil
}

func (s *ComCacheObj) GetError() error {
	return s.scmd.Err()
}

func (s *ComCacheObj) HasError() bool {
	return s.scmd.Err() != nil
}

func (t *ComCacheObj) DecodePipe(obj CacheObj) error {
	cmd, err := t.GetStringCmd()
	if err != nil {
		return err
	}
	js, err := cmd.Result()
	if err != nil {
		return err
	}
	return obj.Decode([]byte(js))
}

func Encode(i interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(i)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte, i interface{}) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode(i)
}

type CacheMapObj interface {
	GetKey() string
	EncodeMap() (map[string]string, error)
	DecodeMap(map[string]string) error
}
