package handlers

import (
	"sync"

	"github.com/CodeWithBenji/a-simple-redis-clone/internals/resp"
)

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

var Handlers = map[string]func([]resp.RespValue) resp.RespValue{
	"PING": ping,
	"SET":  Set,
	"GET":  Get,
	"DEL":  Del,
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
