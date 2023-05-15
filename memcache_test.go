package memcache

import (
	"strconv"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
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
		t.Errorf("didn't get unicode string back %v", v)
	}

	mc.Set(StringItem("str", u))
	if v, ok := mc.GetString("str"); !ok || v != u {
		t.Errorf("didn't get string back %v", v)
	}

	mc.Set(BoolItem("boolean", true))
	if v, ok := mc.GetBool("boolean"); !ok || v != true {
		t.Errorf("Expected true, got: %v", v)
	}
}

func TestItem_String(t *testing.T) {
	mc := NewClient([]string{"127.0.0.1:11211"})

	manualPickledItem := &memcache.Item{
		Key:   "manually_pickled",
		Value: []byte("\x80\x02X\x1c\x00\x00\x00I\xc3\xb1t\xc3\xabrn\xc3\xa2ti\xc3\xb4n\xef\xbf\xbdliz\xc3\xa6ti\xc3\xb8nq\x00."),
		Flags: FLAG_NONE,
	}
	mc.Set(manualPickledItem)
	if mpi, ok := mc.GetString("manually_pickled"); !ok || mpi != "Iñtërnâtiôn�lizætiøn" {
		t.Errorf("Expected Iñtërnâtiôn�lizætiøn, got: %v", mpi)
	}

	pickledItem := &memcache.Item{
		Key:   "pickled",
		Value: []byte("\x80\x02X\x1c\x00\x00\x00I\xc3\xb1t\xc3\xabrn\xc3\xa2ti\xc3\xb4n\xef\xbf\xbdliz\xc3\xa6ti\xc3\xb8nq\x00."),
		Flags: FLAG_PICKLE,
	}
	mc.Set(pickledItem)
	if pi, ok := mc.GetString("pickled"); !ok || pi != "Iñtërnâtiôn�lizætiøn" {
		t.Errorf("Expected Iñtërnâtiôn�lizætiøn, got: %v", pi)
	}

	unicodeItem := UnicodeItem("unicode", "Iñtërnâtiôn�lizætiøn")
	mc.Set(unicodeItem)
	if u, ok := mc.GetString("unicode"); !ok || u != "Iñtërnâtiôn�lizætiøn" {
		t.Errorf("Expected Iñtërnâtiôn�lizætiøn, got: %v", u)
	}

	item := StringItem("string", "Iñtërnâtiôn�lizætiøn")
	mc.Set(item)
	if i, ok := mc.GetString("string"); !ok || i != "Iñtërnâtiôn�lizætiøn" {
		t.Errorf("Expected Iñtërnâtiôn�lizætiøn, got: %v", i)
	}

	invalidItem := &memcache.Item{
		Key:   "invalid",
		Value: []byte("invalid"),
		Flags: FLAG_PICKLE,
	}
	mc.Set(invalidItem)
	if _, ok := mc.GetString("invalid"); ok {
		t.Errorf("Expected invalid, got: %v", ok)
	}
}

func TestItem_Int64(t *testing.T) {
	mc := NewClient([]string{"127.0.0.1:11211"})

	long := &memcache.Item{
		Key:   "long",
		Value: []byte(strconv.FormatInt(int64(1234567890), 10)),
		Flags: FLAG_LONG,
	}
	mc.Set(long)
	if l, ok := mc.GetInt64("long"); !ok || l != 1234567890 {
		t.Errorf("Expected 1234567890, got: %v", l)
	}

	int := &memcache.Item{
		Key:   "int",
		Value: []byte(strconv.FormatInt(int64(1234567890), 10)),
		Flags: FLAG_INTEGER,
	}
	mc.Set(int)
	if l, ok := mc.GetInt64("int"); !ok || l != 1234567890 {
		t.Errorf("Expected 1234567890, got: %v", l)
	}

}
