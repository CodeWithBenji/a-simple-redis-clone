package handlers

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/CodeWithBenji/a-simple-redis-clone/internals/resp"
)

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

var Handlers = map[string]func([]resp.RespValue) resp.RespValue{
	"PING":   ping,
	"SET":    Set,
	"GET":    Get,
	"DEL":    Del,
	"DBSIZE": DbSize,
	"COPY":   Copy,
	"EXISTS": Exists,
	"KEYS":   Keys,
}

func ping(args []resp.RespValue) resp.RespValue {
	if len(args) == 0 {
		return resp.RespValue{Type: "string", String: "PONG"}
	}
	return resp.RespValue{Type: "string", String: args[0].Bulk}
}

func Set(args []resp.RespValue) resp.RespValue {
	if len(args) != 2 {
		return resp.RespValue{Type: "error", String: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return resp.RespValue{Type: "string", String: "OK"}
}

func Get(args []resp.RespValue) resp.RespValue {
	if len(args) != 1 {
		return resp.RespValue{Type: "error", String: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return resp.RespValue{Type: "null"}
	}

	return resp.RespValue{Type: "bulk", Bulk: value}
}

func Del(args []resp.RespValue) resp.RespValue {
	if len(args) != 1 {
		return resp.RespValue{Type: "error", String: "ERR wrong number of arguments for 'del' command"}
	}

	key := args[0].Bulk

	SETsMu.Lock()
	delete(SETs, key)
	SETsMu.Unlock()

	return resp.RespValue{Type: "string", String: "OK"}
}

func DbSize(args []resp.RespValue) resp.RespValue {
	size := strconv.Itoa(len(SETs))
	return resp.RespValue{Type: "string", String: size}
}

func Copy(args []resp.RespValue) resp.RespValue {
	if len(args) != 2 {
		return resp.RespValue{Type: "error", String: "ERR wrong number of arguments for 'copy' command"}
	}
	key := args[0].Bulk
	copy_key := args[1].Bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()
	SETsMu.Lock()
	SETs[copy_key] = value
	SETsMu.Unlock()

	if !ok {
		return resp.RespValue{Type: "null"}
	}
	return resp.RespValue{Type: "string", String: "1"}
}

func Exists(args []resp.RespValue) resp.RespValue {
	if len(args) < 1 {
		return resp.RespValue{Type: "error", String: "ERR wrong number of arguments for 'exists' command"}
	}

	count := len(args)
	fmt.Println(count)
	number_of_exists := 0
	for i := 0; i < count; i++ {
		SETsMu.RLock()
		_, ok := SETs[args[i].Bulk]
		SETsMu.RUnlock()
		if ok {
			number_of_exists++
		}
	}
	return resp.RespValue{Type: "string", String: strconv.Itoa(number_of_exists)}
}

func Keys(args []resp.RespValue) resp.RespValue {
	v := reflect.ValueOf(SETs)
	keys := make([]interface{}, 0, v.Len())
	for _, k := range v.MapKeys() {
		keys = append(keys, k.Interface())
	}
	fmt.Println(keys)

	return resp.RespValue{Type: "array", String: "OK"}
}

// func Keys(m map[int]interface{}) []int {
//     keys := make([]int, len(m))
//     i := 0
//     for k := range m {
//         keys[i] = k
//         i++
//     }
//     return keys
// }
