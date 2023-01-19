package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numPages;
    public static class Frame{
        public int  pin;
        public Page page;
        public Frame(Page page){
            this.pin  = 1;
            this.page = page;
        }
        public Frame(){
            //Default constructor
        }
    }
    private final Frame[] frames;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.frames    = new Frame[numPages];
        for(int i=0;i<numPages;i++){
            this.frames[i] = new Frame();
        }
        this.numPages  = numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int replace=-1;
        //We search through the buffer pool for the page with 'PageId' pid
        Page page=null;
        while(page==null) {
            for (int i = 0; i < numPages; i++) {
                if (frames[i].page == null)
                    replace = replace == -1 ? i : replace;
                else if(frames[i].page.getId().equals(pid)) {
                    //The page is already cached in the buffer pool
                    frames[i].pin++;
                    page=frames[i].page;
                    break;
                }
            }
            //If the page is already cached in the buffer pool,we just return it.
            if(page!=null)
                break;
            //we need to load the page from disk to buffer pool
            if (replace != -1) {
                //not full
                frames[replace] = new Frame(Database.getCatalog().getDatabaseFile(pid.getTableId()).
                        readPage(pid));
                page=frames[replace].page;
            }else{
                //full
                evictPage();
            }
        }
        int times=0;
        int MAX_TRY=new Random().nextInt(20);
        while(!Database.getLockManager().AcquireLock(tid,pid,perm)) {
            try {
                Thread.sleep( 200);
            } catch (Exception e) {
                e.printStackTrace();
            }
            times++;
//            System.out.println("Thread_"+Thread.currentThread().getId()+" Tried times:"+times);
            if(times>MAX_TRY){
                Boolean has_cycle=Database.getLockManager().check_deadlock(tid,pid,perm);
                if(has_cycle) {
                    System.out.println("Thread_"+Thread.currentThread().getId()+" is aborted due to deadlock,have tried "+times);

                    throw new TransactionAbortedException();
                }
                else{
                    times=0;
                    MAX_TRY=new Random().nextInt(20);
                    System.out.println("Thread_"+Thread.currentThread().getId()+" need wait longer, since no cycle related to this thread");
                }
            }
        }
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        Database.getLockManager().release_page(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        return Database.getLockManager().holdsLock(tid,pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        PageId pid;
        try {
            for (Frame frame : frames) {
                if(frame.page!=null) {
                    pid=frame.page.getId();
                    if (frame.page.isDirty() == tid) {
                        if (commit) {
                            flushPage(pid);
                            // use current page contents as the before-image
                            // for the next transaction that modifies this page.
                            frame.page.setBeforeImage();
                        } else {
                            discardPage(pid);
                            Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                        }
                    }
                }
            }
            Database.getLockManager().release_page(tid);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
            List<Page> pages = dbFile.insertTuple(tid, t);
            for (Page page : pages) {
                page.markDirty(true, tid);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            DbFile dbFile = Database.getCatalog().
                    getDatabaseFile(t.getRecordId().getPageId().getTableId());
            List<Page> pages = dbFile.deleteTuple(tid, t);
            for (Page page : pages) {
                page.markDirty(true, tid);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for(Frame frame:frames){
            if(frame.page!=null) {
                flushPage(frame.page.getId());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        for(Frame frame:frames){
            if(frame.page!=null&&frame.page.getId().equals(pid)){
                frame.pin=0;
                frame.page=null;
                break;
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        TransactionId tid;
        for(Frame frame:frames){
            if(frame.page!=null&&frame.page.getId().equals(pid)){
                if((tid=frame.page.isDirty())!=null) {
                    // append an update record to the log, with
                    // a before-image and after-image.
                    Database.getLogFile().logWrite(tid, frame.page.getBeforeImage(), frame.page);
                    Database.getLogFile().force();
                    dbFile.writePage(frame.page);
                    frame.page.markDirty(false,tid);
                }
                break;
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        Random rand = new Random();
        int next=rand.nextInt(numPages);
        int temp=next;
        while(frames[temp].page.isDirty()!=null) {
            temp=(temp+1)%numPages;
            if(temp==next)
                throw new DbException("All pages are dirty,eviction failed!");
        }
        PageId pid=frames[temp].page.getId();
        discardPage(pid);
        Database.getLockManager().release_page(pid);
    }

}
