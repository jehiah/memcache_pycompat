package memcache

// A wrapper around bradfitz/gomemcache that provides compatability with libmemcache and python data types
//
// Key distribution is compatible with libmemcached and consistent ketama hashing
// Values are interchangeable with Python datatypes (integer, string, unicode string) 
// as stored with https://pypi.python.org/pypi/pylibmc

import (
	"bytes"
	"encoding/binary"
	"hash"
	"strconv"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/dgryski/dgohash"
	"github.com/rckclmbr/goketama/ketama"
)

// these flags match pylibmc in _pylibmcmodule.h
const (
	FLAG_NONE    uint32 = 0
	FLAG_PICKLE  uint32 = 1 << 0 // note 'None' and unicoee types in python get set as pickled
	FLAG_INTEGER uint32 = 1 << 1
	FLAG_LONG    uint32 = 1 << 2
	FLAG_ZLIB    uint32 = 1 << 3
	FLAG_BOOL    uint32 = 1 << 4 // This is a pylibmc addition
)

// a memcache Client with python/pylibmc/libmemcache compatability
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

// return a memcache.Client with ketama consistent hashing (non-weighted)
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

// Get a cached string returning weather or not the get was successfull.
func (c *Client) GetString(k string) (string, bool) {
	i, err := c.Get(k)
	if err == nil {
		switch i.Flags {
		case FLAG_PICKLE:
			// Note: unicode objects get pickled, but parsing them is straight forward
			//
			// parsing is based on the following and only checks for protocol version 2
			// - http://spootnik.org/entries/2014/04/05_diving-into-the-python-pickle-format.html
			// - http://www.hydrogen18.com/blog/reading-pickled-data-in-go.html
			// - https://github.com/hydrogen18/stalecucumber
			//
			// record format: (10 bytes in addition to the string)
			// 2 pickle pre-amble - 0x80, 0x2 (pickle flag and version)
			// 1 byte unicode opcode - 0x58
			// 4 byte size - little endian
			// ...
			// 2 byte BINPUT 1 - 0x71, 0x1
			// 1 byte stop opcode  - 0x2e
			unicodePreamble := []byte{0x80, 0x2, 0x58}
			if bytes.HasPrefix(i.Value, unicodePreamble) && len(i.Value) >= 10 {
				size := binary.LittleEndian.Uint32(i.Value[3:])
				if size+10 == uint32(len(i.Value)) {
					return string(i.Value[7 : 7+size]), true
				}
			}
		case FLAG_NONE:
			return string(i.Value), true
		}
	}
	return "", false
}

// Get a cached integer returning weather or not the get was successfull
func (c *Client) GetInt64(k string) (int64, bool) {
	i, err := c.Get(k)
	if err == nil {
		if i.Flags == FLAG_INTEGER {
			n, err := strconv.ParseInt(string(i.Value), 10, 64)
			if err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

// GetBool returns boolean values or integer 0/1 as a boolean value.
func (c *Client) GetBool(k string) (bool, bool) {
	i, err := c.Get(k)
	if err != nil {
		return false, false
	}
	if i.Flags != FLAG_BOOL && i.Flags != FLAG_INTEGER {
		return false, false
	}
	// we allow the integer 0/1 values to be interpreted as boolean
	s := string(i.Value)
	if s == "0" {
		return false, true
	} else if s == "1" {
		return true, true
	}
	return false, false
}

// return a memcache.Item sutable for storing a utf-8 string
// this provides compatability with pylibmc
func StringItem(k, s string) *memcache.Item {
	return &memcache.Item{
		Key:   k,
		Value: []byte(s),
		Flags: FLAG_NONE,
	}
}

// return a memcache.Item with a string stored as a python
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

// return a memcache.Item sutable for storing a boolean
// this provides compatability with pylibmc
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

// return a memcache.Item sutable for storing an int64
// this provides compatability with pylibmc
func Int64Item(k string, v int64) *memcache.Item {
	return &memcache.Item{
		Key:   k,
		Value: []byte(strconv.FormatInt(v, 10)),
		Flags: FLAG_INTEGER,
	}
}
