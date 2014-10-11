package simpledb;

import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
	private HeapFile file;
	private DbFileIterator iterator;
	private int tableNo;
	private String tableAlias;

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String a) {
        file = (HeapFile)Database.getCatalog().getDatabaseFile(tableid);
        iterator = file.iterator(tid);    
        tableNo = tableid;
        tableAlias = a;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableNo);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String alias) {
    	this.tableNo = tableid;
    	this.tableAlias = alias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
    	//iterator = Database.getCatalog().getDatabaseFile(tableId).iterator(transId);
    	iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(this.tableNo);
    	Iterator<TDItem> tupleIt = tupleDesc.iterator();
    	
    	String alias = this.tableAlias;
    	
    	List<String> fields = new ArrayList<String>();
    	List<Type> types = new ArrayList<Type>();
    	
    	while (tupleIt.hasNext()) {
    		TDItem next = tupleIt.next();
    		String currentName = next.fieldName;
    		
    		if (alias==null) {
    			alias = "null";
    		}
    		String newName = alias + "." +  currentName;
    		fields.add(newName);
    		types.add(next.fieldType);
    	}
    	
    	String[] newFields = fields.toArray(new String[fields.size()]);
    	Type[] newTypes = types.toArray(new Type[types.size()]);
    	TupleDesc newTupleDesc = new TupleDesc(newTypes,newFields);
    	
    	return newTupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return (iterator.hasNext());
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return (iterator.next());
    }

    public void close() {
        iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        iterator.rewind();
    }
}
