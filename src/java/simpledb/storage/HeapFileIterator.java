package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;

public class HeapFileIterator extends AbstractDbFileIterator {
    private final int pageNum;
    private final TransactionId tid;
    private final int tableid;
    private int   curPgno=0;
    private Iterator<Tuple> curPage=null;
    HeapFileIterator(TransactionId tid,int tableId,int pageNum){
        this.tid     = tid;
        this.tableid = tableId;
        this.pageNum = pageNum;
        this.curPage = null;
    }
    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        //not open yet
        if(this.curPage==null)
            return null;
        Tuple tuple=null;
        //Handle read out a page
        while(this.curPgno<this.pageNum) {
            if (this.curPage == null)
                this.curPage = ((HeapPage) Database.getBufferPool().
                        getPage(tid, new HeapPageId(this.tableid, this.curPgno), Permissions.READ_ONLY)).iterator();
            if (!this.curPage.hasNext()){
                this.curPage = null;
                this.curPgno++;
            }else{
                tuple = this.curPage.next();
                return tuple;
            }
        }
        return tuple;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.curPage = ((HeapPage) Database.getBufferPool().
                getPage(tid, new HeapPageId(this.tableid, 0), Permissions.READ_ONLY)).iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        this.curPgno=0;
        this.curPage=null;
    }
}
