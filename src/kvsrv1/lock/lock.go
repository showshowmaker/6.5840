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
	ck kvtest.IKVClerk
	// You may add code here
	key string
	id  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.key = l
	lk.id = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// fmt.Println("Acquire")
		value, version, _ := lk.ck.Get(lk.key)
		// value, version, err := lk.ck.Get(lk.key)
		// fmt.Println("Acquire ", value, version, err)
		// fmt.Println(value)
		if value == "" {
			err := lk.ck.Put(lk.key, lk.id, version)
			if err == rpc.OK {
				return
			} else if err == rpc.ErrMaybe {
				value, _, _ = lk.ck.Get(lk.key)
				if value == lk.id {
					return
				}
			}
			// return
		}
		// fmt.Println(value)
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	// value, version, err := lk.ck.Get(lk.key)
	// fmt.Println("Release ", value, version, err)
	value, version, _ := lk.ck.Get(lk.key)
	if value == lk.id {
		for {
			lk.ck.Put(lk.key, "", version)
			value, _, _ := lk.ck.Get(lk.key)
			if value == "" {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}

	}
}
