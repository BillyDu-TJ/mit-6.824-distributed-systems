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
	myID string
	myName string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	lk.myName = lockname
	lk.myID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// polling to get lock
		id, version, err := lk.ck.Get(lk.myName)
		// log.Printf("[%s] Get returned id='%s', ver=%d, err=%v", lk.myID, id, version, err)
		if id == lk.myID {
			// suffered a network error, and resend.
			// thus, we get ErrMaybe, and if we right
			// succeded, but reply lost in the network,
			// out get will return ours' name.
			// this means we have write successfully, 
			// directly return
			return
		}
		if err == rpc.ErrMaybe {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err == rpc.ErrNoKey {
			// create a key
			ok := lk.ck.Put(lk.myName, lk.myID, 0)
			if ok == rpc.OK {
				// successfully acquired the lock
				return
			} else if ok == rpc.ErrVersion {
				// fail, retry
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
		if id != lk.myID && id != "" {
			// lock is holding by others
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if id == "" {
			ok := lk.ck.Put(lk.myName, lk.myID, version)
			if ok == rpc.OK {
				// successfully acquired the lock
				return
			} else if ok == rpc.ErrVersion {
				// fail, retry
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		// get the version first
		id, currentVersion, err := lk.ck.Get(lk.myName)
		if err == rpc.ErrNoKey || id != lk.myID {
			// the lock was released, return
			return
		}
		if id == lk.myID {
			ok := lk.ck.Put(lk.myName, "", currentVersion)
			if ok == rpc.OK {
				return
			} else {
				// network error. retry
				continue
			}
		}	
	}
}
