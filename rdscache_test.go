package rdscache

import (
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/letsfire/redigo/v2/mode/alone"
	"github.com/stretchr/testify/assert"
)

func TestNewDriver(t *testing.T) {
	d := NewDriver("test.cache", alone.NewClient())
	assert.Nil(t, d.Set("test", []byte("123"), time.Second))
	bts, ok, err := d.Get("test")
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, "123", string(bts))

	assert.Nil(t, d.Del("test"))

	ok, err = d.Has("test")
	assert.Nil(t, err)
	assert.False(t, ok)

	_ = d.Set("test2", []byte("123"), time.Second)
	rcd := d.(rdsCacheDriver)
	_, _ = rcd.client.Execute(func(c redis.Conn) (interface{}, error) {
		assert.Nil(t, rcd.gc(c, time.Now().Add(time.Second*2).Unix()))
		return nil, nil
	})
	ok, err = d.Has("test2")
	assert.Nil(t, err)
	assert.False(t, ok)
}
