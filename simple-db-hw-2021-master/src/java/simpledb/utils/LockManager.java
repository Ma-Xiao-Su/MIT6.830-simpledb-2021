package simpledb.utils;

import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    /** Store locks per page. */
    private final Map<PageId, List<Lock>> lockMap;

    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
    }

    /**
     * The transaction tid adds a lock of type lockType to the pageId page,
     * timeout is the maximum time to try to lock.
     *
     * @param pageId
     * @param tid
     * @param lockType
     * @param timeout
     * @return
     *      Returns true if the lock is successful, otherwise returns false.
     */
    public boolean tryAcquireLock(final TransactionId tid, final PageId pageId,
                                  final Permissions lockType, final int timeout) {
        final long startTime = System.currentTimeMillis();
        // spin lock.
        while (true) {
            if (System.currentTimeMillis() - startTime >= timeout) {
                return false;
            }
            if (acquireLock(tid, pageId, lockType)) {
                return true;
            }
        }
    }

    /**
     * The transaction tid adds a lock of type lockType to the pageId page.
     *
     * @param pageId
     * @param tid
     * @param lockType
     * @return
     *      Returns true if the lock is successful, otherwise returns false.
     */
    public synchronized boolean acquireLock(final TransactionId tid, final PageId pageId, final Permissions lockType) {
        // 1. If the page is not locked, lock it directly and return true.
        if (!this.lockMap.containsKey(pageId)) {
            final Lock lock = new Lock(tid, lockType);
            final List<Lock> locks = new ArrayList<>();
            locks.add(lock);
            lockMap.put(pageId, locks);
            return true;
        }

        // The following is the case when the page has a lock:

        final List<Lock> locks = lockMap.get(pageId);
        // 2. Check if this transaction holds the lock.
        for (final Lock lock : locks) {
            // This transaction holds the lock.
            if (lock.getTid().equals(tid)) {
                if (lock.getLockType() == lockType) {
                    return true;
                }
                if (lock.getLockType() == Permissions.READ_WRITE) {
                    return true;
                }
                // The read lock is upgraded to a read-write lock ???
                if (lock.getLockType() == Permissions.READ_ONLY && locks.size() == 1) {
                    lock.setLockType(Permissions.READ_WRITE);
                    return true;
                }
                return false;
            }
        }

        // Other transactions hold locks:
        // 3. Check whether exists a writeLock.
        // Because only one write lock can be added, the element with index 0 is directly taken.
        if (locks.size() > 0 && locks.get(0).getLockType() == Permissions.READ_WRITE) {
            return false;
        }

        // 4. There already exists another locks (READ_ONLY), so we just can get a readLock.
        if (lockType == Permissions.READ_ONLY) {
            final Lock lock = new Lock(tid, lockType);
            locks.add(lock);
            return true;
        }

        // 5. Other transactions hold write locks,
        // and this transaction also wants to add write locks,
        // so the lock fails
        return false;
    }

    /**
     * This transaction releases the lock.
     *
     * @param pageId
     * @param tid
     * @return
     *      Returns true if the lock is released successfully, otherwise returns false.
     */
    public synchronized boolean releaseLock(final TransactionId tid, final PageId pageId) {
        // Page unlocked.
        if (!lockMap.containsKey(pageId)) {
            return false;
        }

        final List<Lock> locks = lockMap.get(pageId);
        for (Lock lock : locks) {
            // Only locks held by this transaction on this page can be released.
            if (lock.getTid().equals(tid)) {
                locks.remove(lock);
                if (locks.size() > 0) {
                    lockMap.put(pageId, locks);
                }
                return true;
            }
        }

        return false;
    }

    /**
     * Check if transaction tid holds lock on PageId page.
     *
     * @param pageId
     * @param tid
     * @return
     */
    public synchronized boolean holdsLock(final TransactionId tid, final PageId pageId) {
        if (!lockMap.containsKey(pageId)) {
            return false;
        }
        final List<Lock> locks = lockMap.get(pageId);
        for (Lock lock : locks) {
            if (lock.getTid().equals(tid)) {
                return true;
            }
        }
        return false;
    }
}
