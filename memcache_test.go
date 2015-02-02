package memcache

import (
	"fmt"
	"testing"
	"time"
)

func TestGetSet(t *testing.T) {
	mc := NewClient([]string{"127.0.0.1:11211"})

	now := time.Now()
	key := fmt.Sprintf("libbitly.memcached.%d", now.UnixNano())
	mc.Set(Int64Item(key, now.UnixNano()))
	v, ok := mc.GetInt64(key)
	if !ok || v != now.UnixNano() {
		t.Errorf("didn't get back expected value %d", v)
	}
}
