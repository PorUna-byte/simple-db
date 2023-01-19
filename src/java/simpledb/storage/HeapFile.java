package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private final File      file;
    private final TupleDesc schema;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file=f;
        schema=td;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = BufferPool.getPageSize()*pid.getPageNumber();
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            byte[] temp = new byte[(int) Math.min(BufferPool.getPageSize(),raf.length()-offset)];
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.readFully(temp);
            System.arraycopy(temp,0,data,0,temp.length);
            return new HeapPage((HeapPageId)pid,data);
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        int offset = BufferPool.getPageSize()*page.getId().getPageNumber();
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(offset);
            raf.write(page.getPageData());
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) ((file.length()+BufferPool.getPageSize()-1)/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        Boolean done=false;
        HeapPage page=null;
        for(int pgNo=0;pgNo<numPages();pgNo++){
            page=(HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(getId(),pgNo),Permissions.READ_WRITE);
            if(page.getNumEmptySlots()>0) {
                page.insertTuple(t);
                done=true;
                break;
            }
            Database.getLockManager().release_page(tid, page.getId());
        }
        if(!done){
            // If no such pages exist in the HeapFile(i.e. all pages are full)
            // you need to create a new page and append it to the physical file on disk.
            HeapPage new_page = new HeapPage(new HeapPageId(getId(),numPages()),HeapPage.createEmptyPageData());
            //To clear the position on the disk
            writePage(new_page);
            //We must get this page again via buffer pool
            page=(HeapPage)Database.getBufferPool().getPage(tid,new_page.getId(),Permissions.READ_WRITE);
            page.insertTuple(t);
        }
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        Page page=Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
        ((HeapPage)page).deleteTuple(t);
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid,getId(),numPages());
    }

}

