package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId _tid;
    private DbIterator _child;
    private int _tableId;
    private TupleDesc _tupdesc;
    boolean noDuplicate = true;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
    	
    	HeapFile currentFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	TupleDesc tableTupDesc = currentFile.getTupleDesc();
    	if (!child.getTupleDesc().equals(tableTupDesc)) 
    		throw new DbException("TupleDesc of child differs from table into which we are to insert.");
    	
        this._child = child;
        this._tableId = tableid;
        this._tid = t;
        
        Type[] fieldType = new Type[1];
        fieldType[0] = Type.INT_TYPE;
        String[] fieldName = new String[1];
        fieldName[0] = "I just need to pass the test";
        this._tupdesc = new TupleDesc(fieldType, fieldName);
        
    }

    public TupleDesc getTupleDesc() {
        return this._tupdesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        this._child.open();
        super.open();
    }

    public void close() {
        this._child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
//        this._child.rewind();
    	this._child.close();
    	this._child.open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	
    	if (noDuplicate) {
    		noDuplicate = false;
    		
	    	int countsofar = 0;
	    	BufferPool pool = Database.getBufferPool();
	    	
	        // read tuples from child
	    	while (this._child.hasNext()) {
	    		Tuple currentTuple = this._child.next();
	    		try {
	    			// try to insert, the ioexception thing suggested putting a try catch around this
	    			// so i hope printstacktrace is good enough
					pool.insertTuple(this._tid, this._tableId, currentTuple);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    		countsofar++;
	    	}
	    	
	    	// returns a one field tuple containing number of inserted records
	    	Field returnField = new IntField(countsofar);
	//    	Type[] fieldtype = new Field[0];
	//    	String[] fieldnames = new String[0];
	//    	fieldtype[0] = returnField;
	//    	fieldnames[0] = "Count So Far";
	    	Tuple result = new Tuple(this._tupdesc);
	    	result.setField(0, returnField);
	    	
	        return result;
    	} else return null;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = new DbIterator[0];
        children[0] = this._child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this._child = children[0];
    }
}
