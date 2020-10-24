package rdscache

import (
	"testing"
	"time"

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

	_, ok, err = d.Get("test")
	assert.Nil(t, err)
	assert.False(t, ok)

	_ = d.Set("test2", []byte("123"), time.Second)
	rcd := d.(rdsCacheDriver)
	assert.Nil(t, rcd.gc(time.Now().Add(time.Second*2).Unix()))
	_, ok, err = d.Get("test2")
	assert.Nil(t, err)
	assert.False(t, ok)
}
