package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId _transId;
    private DbIterator _child;
    private TupleDesc _tupdesc;
    private boolean noDuplicate = true;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this._child = child;
        this._transId = t;
        
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
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this._child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
	    			
					pool.deleteTuple(_transId, currentTuple);
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
