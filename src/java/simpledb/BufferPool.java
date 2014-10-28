package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int maxPages;
    // using a concurrenthashmap because it was imported for us... xD
    private ConcurrentHashMap <PageId, Page> pool;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
       this.maxPages = numPages;
       this.pool = new ConcurrentHashMap <PageId, Page>();
    }
    
    public static int getPageSize() {
      return PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
//    public  Page getPage(TransactionId tid, PageId pid, Permissions perm) 
//    	throws TransactionAbortedException, DbException {
//    	//note to self: figure out how does the DbException work? when is it thrown?
//    	
//		Page page;
//	    // look to see if pid is already in buffer pool
//		// if yes, return the page from the pool
//		if (pool.containsKey(pid)) {
//			page = pool.get(pid); 
//			
//		} else {
//			// if no, find the page in the database (probably on disk)
//			DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
//			// actually read the page
//			page = file.readPage(pid);
//			// put the page in the pool
//			pool.put(pid, page);
//		}
//		return page;
//		}
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        	Page p;
            synchronized(this) {
                p = pool.get(pid);
                if (p == null) {
                    if (pool.size() >= maxPages) {
                        this.evictPage();
                    }
                    p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    pool.put(pid, p);
                }
            }
            return p;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // add a tuple to the specified table on behalf of transaction id
    	
    	// To add a tuple to a table, we have to find the file
    	HeapFile desiredFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    	// heapfile insert returns an arraylist of changed pages
    	ArrayList<Page> desiredPages = desiredFile.insertTuple(tid, t);
    	
    	// marks any pages that were modified as dirty
    	int numPages = desiredPages.size();
    	for (int i = 0; i < numPages; i++) {
    		
    		Page desiredPage = desiredPages.get(i);
    		desiredPage.markDirty(true, tid);
    		
        	// update cached versions of any pages that have been dirtied
        	// --> meaning update the buffer pool
    		pool.put(desiredPage.getId(), desiredPage);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // remove tuple from buffer pool
    	
    	// to find the file, we have to find the table id from the tuple
    	int tableId = t.getRecordId().getPageId().getTableId();
    	// To add a tuple to a table, we have to find the file
    	HeapFile desiredFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    	// heapfile insert returns an arraylist of changed pages
    	ArrayList<Page> desiredPages = desiredFile.deleteTuple(tid, t);
    	
    	// marks any pages that were modified as dirty
//    	int numPages = desiredPages.size();
//    	for (int i = 0; i < numPages; i++) {
//    		
//    		Page desiredPage = desiredPages.get(i);
//    		desiredPage.markDirty(true, tid);
    		
        	// update cached versions of any pages that have been dirtied
        	// --> meaning update the buffer pool
//    		pool.put(desiredPage.getId(), desiredPage);
//    	}
    	
    	HeapPage desiredPage = (HeapPage) desiredPages.get(0);
    	desiredPage.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // we have a function called flush page
    	// so we can just call that
    	// on all the pages in the bufferpool
    	
    	// find pids because that's what flush page takes in
    	// pages are denoted by their pids
    	// and bufferpool is represented by a map so just get all keys of bufferpool
    	Set<PageId> pages = pool.keySet();
    	Iterator<PageId> currentPage = pages.iterator();
    	while (currentPage.hasNext()) this.flushPage(currentPage.next());

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // write dirty page to disk
    	Page currentPage = this.pool.get(pid);
    	
    	if (currentPage.isDirty() != null) {
	    	HeapFile writeToFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
	    	writeToFile.writePage(currentPage);
	    	// mark as not dirty 
	    	currentPage.markDirty(false, null);
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
//        // Need to generate a random page from all the pages in the buffer pool
//    	// referencing: http://stackoverflow.com/questions/1247915/how-to-generate-a-random-number-with-java-from-given-list-of-numbers
//    	
//    	// so grab all the page ids:
    	List<PageId> pagelist = new ArrayList<PageId>(pool.keySet());
    	int arraySize = pagelist.size();
    	Random random = new Random();
    	int randomNum = random.nextInt(arraySize);
    	PageId pid = pagelist.get(randomNum);

    	if (pool.get(pid).isDirty() != null) {
        	try {
    			if (pool.get(pid).isDirty() != null) flushPage(pid);
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    	pool.remove(pid);
    }
}
