package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	ck       kvtest.IKVClerk
	lockKey  string
	clientId string
}

func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockKey:  l,
		clientId: kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey || (err == rpc.OK && val == "") {
			err := lk.ck.Put(lk.lockKey, lk.clientId, ver)
			if err == rpc.OK {
				return
			}
		} else if err == rpc.OK && val == lk.clientId {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.lockKey)
		if err == rpc.OK {
			if val == lk.clientId {
				err = lk.ck.Put(lk.lockKey, "", ver)
				if err == rpc.OK {
					// 成功释放
					return
				}

			} else {
				return
			}
		} else if err == rpc.ErrNoKey {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
