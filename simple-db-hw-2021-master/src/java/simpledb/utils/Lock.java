package simpledb.utils;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

public class Lock {
    /** Locked transaction tid. */
    private TransactionId tid;

    /** type of lock */
    private Permissions lockType;

    public Lock(final TransactionId tid, final Permissions lockType) {
        this.tid = tid;
        this.lockType = lockType;
    }

    public TransactionId getTid() {
        return tid;
    }

    public Permissions getLockType() {
        return lockType;
    }

    public void setLockType(Permissions lockType) {
        this.lockType = lockType;
    }
}
