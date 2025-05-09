package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	store map[string]ValueVersion // key -> (value, version)
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{store: make(map[string]ValueVersion)}

	return kv
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if valVer, ok := kv.store[args.Key]; ok {
		reply.Value = valVer.Value
		reply.Version = valVer.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	valVer, exists := kv.store[args.Key]
	if exists {
		if valVer.Value == "" && args.Version == 0 {
			kv.store[args.Key] = ValueVersion{Value: args.Value, Version: valVer.Version + 1}
			reply.Err = rpc.OK
		} else if valVer.Version == args.Version {
			kv.store[args.Key] = ValueVersion{Value: args.Value, Version: valVer.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.store[args.Key] = ValueVersion{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
