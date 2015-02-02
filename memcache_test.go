package memcache

import (
	"testing"
	"time"
)

func TestGetSet(t *testing.T) {
	mc := NewClient([]string{"127.0.0.1:11211"})

	now := time.Now()
	mc.Set(Int64Item("int", now.UnixNano()))
	if v, ok := mc.GetInt64("int"); !ok || v != now.UnixNano() {
		t.Errorf("didn't get back expected value %v", v)
	}

	u := "Iñtërnâtiôn�lizætiøn"
	mc.Set(UnicodeItem("unicode", u))
	if v, ok := mc.GetString("unicode"); !ok || v != u {
		t.Errorf("did't get unicode string back %v", v)
	}

	mc.Set(StringItem("str", u))
	if v, ok := mc.GetString("str"); !ok || v != u {
		t.Errorf("did't get string back %v", v)
	}

}
