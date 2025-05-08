package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockKey  string
	clientId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockKey:  l,
		clientId: kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		err := lk.ck.Put(lk.lockKey, lk.clientId, 0)
		if err == rpc.OK {
			return
		}
		val, _, err2 := lk.ck.Get(lk.lockKey)
		if err2 == rpc.OK && val == lk.clientId {
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
