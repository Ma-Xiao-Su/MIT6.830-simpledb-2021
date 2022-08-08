package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.utils.HeapFileIterator;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc td;
    private RandomAccessFile randomAccessFile;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        try {
            this.randomAccessFile = new RandomAccessFile(this.file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     *
     * @param pid
     * @return
     */
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pos = BufferPool.getPageSize() * pid.getPageNumber();
        byte[] byteData = new byte[BufferPool.getPageSize()];
        try {
            randomAccessFile.seek(pos);
            randomAccessFile.read(byteData, 0, byteData.length);
            HeapPage heapPage = new HeapPage((HeapPageId) pid, byteData);
            return heapPage;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param page
     * @throws IOException
     */
    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNo = page.getId().getPageNumber();
        if (pageNo > numPages()) {
            throw new IOException("Over file.");
        }
        int pos = BufferPool.getPageSize() * pageNo;
        randomAccessFile.seek(pos);
        randomAccessFile.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    /**
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> dirtyPageList = new ArrayList<>();
        for (int i = 0; i < numPages(); ++i) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page != null && page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                dirtyPageList.add(page);
                break;
            }
        }

        // all pages are full, so create a page
        if (dirtyPageList.size() == 0) {
            HeapPageId heapPageId = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            writePage(newPage);
            newPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            dirtyPageList.add(newPage);
        }

        return dirtyPageList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> dirtyTupleList = new ArrayList<>();
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        if (page != null && page.isSlotUsed(recordId.getTupleNumber())) {
            page.deleteTuple(t);
            dirtyTupleList.add(page);
        }
        return dirtyTupleList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(numPages(), tid, getId());
    }
}

