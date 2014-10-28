package simpledb;

import java.io.*;

import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	File file;
	private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	try {
	        // page number * number of bytes per page is offset
	    	int pageNo = pid.pageNumber();
	    	int pageSize = BufferPool.PAGE_SIZE;
	    	int offset = pageNo * pageSize;
	    	
	    	//we need something in order to hold the page
	    	byte[] pageBytes = new byte[pageSize];
	    	
	    	// the hint said something about random access so I googled Random Access and Java
	    	// and RandomAccessFiles came up so let's see where this goes
	    	RandomAccessFile randoFile = new RandomAccessFile(this.file, "r");
	    	
	    	// try to get the page, this needs to be surrounded by a try/catch so I guess I'll do that
	    	randoFile.seek(offset);
	    	
	    	//then try to read the data
	    	randoFile.read(pageBytes, 0, pageSize);
	    	randoFile.close();
	    	
	    	// pass bytes array to heappage constructor
	    	HeapPage result = new HeapPage((HeapPageId) pid, pageBytes);
	    	return result;
    	} catch (FileNotFoundException e) {
    		e.getStackTrace();
    	} catch (IOException e) {
    		e.getStackTrace();
    	}
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile randFile = new RandomAccessFile(file, "rws"); //so we can read and write and seek
        PageId pid = page.getId();
        int pageNo = pid.pageNumber();
        randFile.seek(pageNo * BufferPool.PAGE_SIZE);
        randFile.write(page.getPageData());
        randFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	//we have to calculate the number of pages by bytes
    	long totalBytes = file.length();
    	int numPages = (int) Math.ceil(totalBytes/BufferPool.PAGE_SIZE);
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	
        //* @throws IOException if the needed file can't be read/written TODO

    	
    	// create an array list of pages because that's what we need to return
    	ArrayList<Page> result = new ArrayList<Page>();
    	
    	boolean inserted = false;
    	int numPages = this.numPages();
//    	System.out.println("number of pages is " + numPages);

        // loop through the pages
    	for (int potentialPage = 0; potentialPage < numPages; potentialPage++) {
//    		System.out.println("looping thru page");

    		// call current page from buffer pool
    		PageId currentPid = new HeapPageId(this.getId(), potentialPage);
    		BufferPool buffer = Database.getBufferPool();
            HeapPage currentPage = (HeapPage) buffer.getPage(tid, currentPid, Permissions.READ_WRITE);
           
        	// if there is space, then call page insert on that page
            if (currentPage.getNumEmptySlots() > 0) {  
//        		System.out.println("num empty slots is " + currentPage.getNumEmptySlots());

            	currentPage.insertTuple(t);
            	inserted = true;
            	result.add(currentPage);
            	
            	// return the page
                return result;
            }	
    	}	
    	    	
    	// if we need to create a page
    	if (!inserted) {
//    		System.out.println("creating new page");
    		// create a page with the correct page number
    		HeapPageId newPid = new HeapPageId(this.getId(), numPages);
    		HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
    		
    		// then just use page insert to add the tuple to that page
    		newPage.insertTuple(t);
    		
    		// then we have to actually add the new page to the entire file
    		// by grabbing the file and editing it
          RandomAccessFile randFile = new RandomAccessFile(file, "rw"); // lets us read and write to file
          randFile.seek(numPages * BufferPool.PAGE_SIZE);
          randFile.write(newPage.getPageData(), 0, BufferPool.PAGE_SIZE);
          randFile.close();
            result.add(newPage);
            
            // return result
            return result;
            
    	}
    	
        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        
    	// create an array list to return
    	ArrayList<Page> result = new ArrayList<Page>();
    	// find the page the tuple lives on
    	PageId pageid = t.getRecordId().getPageId();
		BufferPool buffer = Database.getBufferPool();
        HeapPage currentPage = (HeapPage) buffer.getPage(tid, pageid, Permissions.READ_WRITE);
    	
    	// call page deleteTuple
        currentPage.deleteTuple(t);
        result.add(currentPage);
        return result;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

    	return new HeapFileIterator(this,tid);
    }
    
    class HeapFileIterator implements DbFileIterator {

    	private Tuple next = null;
        Iterator<Tuple> it = null;
        int curpgno = 0;

        TransactionId tid;
        HeapFile hf;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.hf = hf;
            this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException {
            curpgno = -1;
        }
        
    	public boolean hasNext() throws DbException, TransactionAbortedException {
            if (next == null) next = readNext();
            return next != null;
        }

        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if (next == null) {
                next = readNext();
                if (next == null) throw new NoSuchElementException();
            }

            Tuple result = next;
            next = null;
            return result;
        }

        Tuple readNext() throws TransactionAbortedException, DbException {
            if (it != null && !it.hasNext())
                it = null;

            while (it == null && curpgno < hf.numPages() - 1) {
                curpgno++;
                HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
                HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
                        curpid, Permissions.READ_ONLY);
                it = curp.iterator();
                if (!it.hasNext())
                    it = null;
            }

            if (it == null)
                return null;
            return it.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
        	next = null;
            it = null;
            curpgno = Integer.MAX_VALUE;
        }
    }
}

