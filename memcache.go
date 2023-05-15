package memcache

// A wrapper around bradfitz/gomemcache that provides compatibility with libmemcache and python data types
//
// Key distribution is compatible with libmemcached and consistent ketama hashing
// Values are interchangeable with Python datatypes (integer, string, unicode string)
// as stored with https://pypi.python.org/pypi/pylibmc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/dgryski/dgohash"
	"github.com/nlpodyssey/gopickle/pickle"
	"github.com/rckclmbr/goketama/ketama"
)

// these flags match pylibmc in _pylibmcmodule.h
const (
	FLAG_NONE    uint32 = 0
	FLAG_PICKLE  uint32 = 1 << 0 // note 'None' and unicode types in python get set as pickled
	FLAG_INTEGER uint32 = 1 << 1
	FLAG_LONG    uint32 = 1 << 2
	FLAG_ZLIB    uint32 = 1 << 3
	FLAG_BOOL    uint32 = 1 << 4 // https://github.com/lericson/pylibmc/issues/242
)

// Client wraps a memcache Client with python/pylibmc/libmemcache compatibility
type Client struct {
	*memcache.Client
}

// Since we use non-weighted ketama, this provides the Jenkins one-at-a-time hash
// function to ketama. (When using weighted libmemcached chooses md5)
func ketamaDigest() hash.Hash {
	return dgohash.NewJenkins32()
}

// create an address struct that fulfills net.Addr while still returning hostnames
type hostAddress struct {
	hostport string
}

func (a *hostAddress) Network() string { return "tcp" }
func (a *hostAddress) String() string  { return a.hostport }

// NewClient returns a memcache.Client with ketama consistent hashing (non-weighted)
func NewClient(addresses []string) *Client {
	var servers []ketama.ServerInfo
	for _, endpoint := range addresses {
		var serverWeight uint64
		// construct our own address instead of net.ResolveTCPAddress since we want to
		// keep hostnames for hashing instead of the actual ip address
		addr := &hostAddress{endpoint}
		servers = append(servers, ketama.ServerInfo{addr, serverWeight})
	}
	continuum := ketama.New(servers, ketamaDigest)
	return &Client{memcache.NewFromSelector(continuum)}
}

type Item struct {
	*memcache.Item
}

var InvalidType error = errors.New("Invalid Value Type")

// GetString gets k from cache returning whether or not the get was successful
func (c *Client) GetString(k string) (string, bool) {
	i, err := c.Get(k)
	if err == nil {
		s, err := (&Item{i}).String()
		if err == nil {
			return s, true
		}
	}
	return "", false
}

// String returns the compatible python string value
func (i *Item) String() (string, error) {
	switch i.Flags {
	case FLAG_PICKLE:
		s, err := unpickle(string(i.Value))
		if err != nil {
			return "", err
		}
		return s.(string), nil
	case FLAG_NONE:
		if bytes.HasPrefix(i.Value, []byte{0x80, 0x2}) {
			s, err := unpickle(string(i.Value))
			if err != nil {
				return "", err
			}
			return s.(string), nil
		}
		return string(i.Value), nil
	}
	return "", InvalidType
}

// GetInt64 gets an int64 from cache returning whether or not the get was successful
func (c *Client) GetInt64(k string) (int64, bool) {
	i, err := c.Get(k)
	if err == nil {
		n, err := (&Item{i}).Int64()
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

// Int64 returns the compatible python int value
func (i *Item) Int64() (int64, error) {
	if i.Flags == FLAG_INTEGER || i.Flags == FLAG_LONG {
		n, err := strconv.ParseInt(string(i.Value), 10, 64)
		if err == nil {
			return n, nil
		}
		return 0, err
	}
	return 0, InvalidType
}

// Bool returns the python compatible boolean.
func (i *Item) Bool() (bool, error) {
	if i.Flags != FLAG_BOOL && i.Flags != FLAG_INTEGER {
		return false, InvalidType
	}

	// we allow the integer 0/1 values to be interpreted as boolean
	s := string(i.Value)
	if s == "0" {
		return false, nil
	} else if s == "1" {
		return true, nil
	}
	return false, errors.New("Invalid Boolean Value")
}

// GetBool returns boolean values or integer 0/1 as a boolean value.
func (c *Client) GetBool(k string) (bool, bool) {
	i, err := c.Get(k)
	if err == nil {
		b, err := (&Item{i}).Bool()
		if err == nil {
			return b, true
		}
	}
	return false, false
}

// StringItem returns a memcache.Item suitable for storing a utf-8 string
// this provides compatability with pylibmc
func StringItem(k, s string) *memcache.Item {
	return &memcache.Item{
		Key:   k,
		Value: []byte(s),
		Flags: FLAG_NONE,
	}
}

// UnicodeItem returns a memcache.Item with a string stored as a python
// picked unicode object
func UnicodeItem(k, s string) *memcache.Item {
	size := len(s)
	b := make([]byte, size+10)
	b[0] = 0x80 // 2 byte pickle pre-amble - 0x80, 0x2 (pickle flag and version)
	b[1] = 0x2
	b[2] = 0x58 // 1 byte unicode opcode - 0x58
	// 4 byte size - little endian
	binary.LittleEndian.PutUint32(b[3:], uint32(len(s)))
	copy(b[7:], s)
	b[size+7] = 0x71 // 2 byte BINPUT 1 - 0x71, 0x1
	b[size+8] = 0x1
	b[size+9] = 0x2e // 1 byte stop opcode  - 0x2e
	return &memcache.Item{
		Key:   k,
		Value: b,
		Flags: FLAG_PICKLE,
	}
}

// BoolItem returns a memcache.Item suitable for storing a boolean
// this provides compatability with pylibmc
// to maintain compatibility between python2 and python3,
// the values are pickled as True or False, rather than 1 or 0
// In turn, go will unpickle this value whenever it is set.
func BoolItem(k string, v bool) *memcache.Item {
	value := "0"
	if v {
		value = "1"
	}
	return &memcache.Item{
		Key:   k,
		Value: []byte(value),
		Flags: FLAG_BOOL,
	}
}

// Int64Item returns a memcache.Item sutable for storing an int64
// this provides compatability with pylibmc
func Int64Item(k string, v int64) *memcache.Item {
	return &memcache.Item{
		Key:   k,
		Value: []byte(strconv.FormatInt(v, 10)),
		Flags: FLAG_INTEGER,
	}
}

func unpickle(s string) (interface{}, error) {
	pickledData := strings.NewReader(s)
	unpickler := pickle.NewUnpickler(pickledData)
	value, err := unpickler.Load()
	if err != nil {
		return "", err
	}
	return value, nil
}
