package simpledb.transaction;
import simpledb.storage.PageId;
import simpledb.common.Permissions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

class PageId_Lock{
    public PageId pageId;
    public Permissions permissions;
    public PageId_Lock(PageId pageId, Permissions permissions){
        this.pageId=pageId;
        this.permissions=permissions;
    }
}
class Tid_Lock{
    public TransactionId tid;
    public Permissions permissions;
    public Tid_Lock(TransactionId tid, Permissions permissions){
        this.tid=tid;
        this.permissions=permissions;
    }
}

public class LockManager{
    //This map records all locks owned by a specific transaction
    private final Map<TransactionId, CopyOnWriteArrayList<PageId_Lock>> Transaction2page;
    //This map records all transactions that have a shared lock on a specific page
    //or single transaction that have an exclusive lock on a specific page
    private final Map<PageId,CopyOnWriteArrayList<Tid_Lock>> Page2Transaction;
    public LockManager(){
        Transaction2page = new HashMap<>();
        Page2Transaction = new HashMap<>();
    }
    /**
     * This method is called by a transaction to acquire a lock on a page
     * If this page can be granted, then return true, otherwise return false
     * */
     public synchronized boolean AcquireLock(TransactionId tid,PageId pid,Permissions perm){
//        System.out.println("Thread_"+Thread.currentThread().getId()+" Try to acquire lock");
        //First we need to check whether the Transaction has a lock on the page
        if(Transaction2page.get(tid)!=null){
            for(PageId_Lock pageId_lock:Transaction2page.get(tid)){
                if(pageId_lock.pageId.equals(pid)){
                    //If the transaction already has the lock on the page,do nothing
                    if(pageId_lock.permissions==perm)
                        return true;
                    else if(pageId_lock.permissions==Permissions.READ_WRITE&&perm==Permissions.READ_ONLY){
                        //we just degrade the lock
                        Page2Transaction.get(pid).get(0).permissions=perm;
                        pageId_lock.permissions=perm;
                        return true;
                    }
                }
            }
        }
        if(Page2Transaction.get(pid)!=null){
            for(Tid_Lock tid_lock:Page2Transaction.get(pid)){
                //other transaction have exclusive-lock
                if(tid_lock.permissions==Permissions.READ_WRITE)
                    return false;
                //Acquire a exclusive lock
                if(perm==Permissions.READ_WRITE){
                    //other transaction have a lock
                    if(!tid_lock.tid.equals(tid))
                        return false;
                    else{
                        if(Page2Transaction.get(pid).size()==1){
                            //Only the transaction have a read-lock
                            //just upgrade it
                            Page2Transaction.get(pid).get(0).permissions=perm;
                            for(PageId_Lock pageId_lock:Transaction2page.get(tid)){
                                if(pageId_lock.pageId.equals(pid)){
                                    pageId_lock.permissions=perm;
                                }
                            }
                            return true;
                        }else{
                            //other transaction have a lock
                            return false;
                        }
                    }
                }
            }
            //Acquire a shared-lock
            Page2Transaction.get(pid).add(new Tid_Lock(tid,perm));
            Transaction2page.computeIfAbsent(tid, k -> new CopyOnWriteArrayList<>()).add(new PageId_Lock(pid,perm));
            return true;
        }else{
            //This page is not locked by any transaction
            Page2Transaction.put(pid,new CopyOnWriteArrayList<>());
            Page2Transaction.get(pid).add(new Tid_Lock(tid,perm));
            Transaction2page.computeIfAbsent(tid, k -> new CopyOnWriteArrayList<>()).add(new PageId_Lock(pid,perm));
            return true;
        }
    }
    //A transaction release its lock on a page
     public synchronized void release_page(TransactionId tid,PageId pid){
         if(Page2Transaction.get(pid)==null)
             return;
         if(Transaction2page.get(tid)==null)
             return;
        Page2Transaction.get(pid).removeIf(tid_lock -> tid_lock.tid.equals(tid));
        Transaction2page.get(tid).removeIf(pid_lock -> pid_lock.pageId.equals(pid));
    }
    //check if a transaction holds a lock on a page
     public synchronized boolean holdsLock(TransactionId tid,PageId pid){
        if(Transaction2page.get(tid)==null)
            return false;
        for(PageId_Lock pageId_lock:Transaction2page.get(tid)){
            if(pageId_lock.pageId == pid)
                return true;
        }
        return false;
    }
    //release all locks on a page
     public synchronized void release_page(PageId pid){
        if(Page2Transaction.get(pid)==null)
            return;
        for(Tid_Lock tid_lock:Page2Transaction.get(pid)){
            release_page(tid_lock.tid,pid);
        }
    }
    //release all locks of a transaction
    public synchronized void release_page(TransactionId tid){
        if(Transaction2page.get(tid)==null)
            return;
        for(PageId_Lock pid_lock:Transaction2page.get(tid)){
            release_page(tid, pid_lock.pageId);
        }
    }
    //This method check if there is a deadlock related to transaction 'tid'
    public synchronized Boolean check_deadlock(TransactionId tid,PageId pid,Permissions perm){
        //build graph
        Map<TransactionId, CopyOnWriteArraySet<TransactionId>> graph=new HashMap<>();
        PageId_Lock temp_pidlock=new PageId_Lock(pid,perm);
        Tid_Lock temp_tidlock=new Tid_Lock(tid,perm);
        //We add two temporary locks
        Transaction2page.computeIfAbsent(tid, k -> new CopyOnWriteArrayList<>()).add(temp_pidlock);
        Page2Transaction.computeIfAbsent(pid, k -> new CopyOnWriteArrayList<>()).add(temp_tidlock);
        for(TransactionId tid_t:Transaction2page.keySet()){
            for(PageId_Lock pageId_lock:Transaction2page.get(tid_t)){
                for(Tid_Lock tid_lock:Page2Transaction.get(pageId_lock.pageId)){
                    if(tid_lock.tid!=tid_t&&(pageId_lock.permissions==Permissions.READ_WRITE||tid_lock.permissions==Permissions.READ_WRITE)){
                        if(graph.get(tid_t)==null) {
                            graph.put(tid_t,new CopyOnWriteArraySet<>());
                            graph.get(tid_t).add(tid_lock.tid);
                        }else {
                            graph.get(tid_t).add(tid_lock.tid);
                        }
                    }
                }
            }
        }
        //detect cycle related to tid, use deep-first search on the directed graph
        HashMap<TransactionId,Boolean> visited=new HashMap<>();
        Boolean has_cycle=DFS(graph,visited,tid);
        //we remove two temporary locks
        Transaction2page.get(tid).remove(temp_pidlock);
        Page2Transaction.get(pid).remove(temp_tidlock);
        return has_cycle;
    }
    //If there is a cycle,DFS will return true
    private Boolean DFS(Map<TransactionId, CopyOnWriteArraySet<TransactionId>> graph,HashMap<TransactionId,Boolean> visited,TransactionId tid){
        visited.put(tid,true);
        Boolean flag=false;
        if(graph.get(tid)==null)
            return false;
        for(TransactionId tid_t:graph.get(tid)){
            if(visited.get(tid_t)!=null&&visited.get(tid_t))
                return true;
            flag = flag || DFS(graph,visited,tid_t);
        }
        return flag;
    }
}
