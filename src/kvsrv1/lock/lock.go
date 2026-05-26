package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	id       string
	lockname string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	id := kvtest.RandValue(8)
	lk := &Lock{ck: ck,
		id:       id,
		lockname: lockname,
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		nowId, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey || (err == rpc.OK && nowId == "") {
			err = lk.ck.Put(lk.lockname, lk.id, version)
			if err == rpc.OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (lk *Lock) Release() {
	// Your code here
	nowId, version, err := lk.ck.Get(lk.lockname)
	if err == rpc.OK && nowId == lk.id {
		lk.ck.Put(lk.lockname, "", version)
	}
}
