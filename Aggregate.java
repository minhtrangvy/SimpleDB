package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator _child;
    private int _afield;
    private int _gfield;
    private Aggregator.Op _op;
    
    // we need to figure out what kind of aggregator we have
    private Aggregator aggregator;
    // we need an iterator to iterate through the actual aggregator
    private DbIterator it;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	
    	// Set everything
        this._child = child;
        this._afield = afield;
        this._gfield = gfield;
        this._op = aop;
        
        // Check for grouping in order to find groupby type
        Type groupbytype; 
        if (gfield == Aggregator.NO_GROUPING) groupbytype = null;
        else groupbytype = child.getTupleDesc().getFieldType(gfield);
        
        // Then we also need the aggregate type
        Type aggregateType = child.getTupleDesc().getFieldType(afield);
        // So we can make the right aggregator (of the right type)
        if(aggregateType == Type.INT_TYPE) aggregator = new IntegerAggregator(gfield, groupbytype, afield, aop);
        else aggregator = new StringAggregator(gfield, groupbytype, afield, aop);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
//    	if (this._op == NO_GROUPING) return Aggregator.NO_GROUPING;
//    	else 
    	return this._gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if (this._gfield == Aggregator.NO_GROUPING) return null;
    	else return this._child.getTupleDesc().getFieldName(this._gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
//    	if (this._gfield == Aggregator.NO_GROUPING) return null;
//    	else return this._child.getTupleDesc().getFieldName(this._gfield);
    	return this._afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		return this._child.getTupleDesc().getFieldName(this._afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		return this._op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	
    	// we want to open the child iterator
        this._child.open();
        
        // the tests said that there was a mismatch
        // so i guess we have to put all the child tuples
        // into the aggregator 
        while (this._child.hasNext()){
                aggregator.mergeTupleIntoGroup(this._child.next());
        }
        
        it = aggregator.iterator();
        it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. (taken care of by the DbIterator implementation already)
     * 
     * Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// we need to check if the iterator has been opened
    	if (it == null) throw new DbException("open() not called before fetchNext()");
    	
    		// if there is a next tuple, we just want to get the next one
    		// everything should already be handled by the DbIterator implementation
            if (it.hasNext()) return it.next();
            
            // else we don't have anything left and should return null
            else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	
    	// if NO_GROUPING
    	// return a tuple desc with only one field (aggregate)
    	if (this._gfield == Aggregator.NO_GROUPING) {
        	// for any tupleDesc, we need the fieldtype array and the fieldname arrays
        	Type[] fieldType = new Type[1];
        	String[] fieldName = new String[1];
        	fieldType[0] = this._child.getTupleDesc().getFieldType(_afield);
        	//  "aggName(aop) (child_td.getFieldName(afield))"
        	fieldName[0] = this._op.toString() + 
        				   "(" + this._child.getTupleDesc().getFieldName(this._afield) + ")";
        	return new TupleDesc(fieldType, fieldName);
    	}
    	
    	// else, we want to return the first field = groupbyfield
    	// second field = aggregate value column: aggname(aop) afieldname
    	else {
        	// for any tupleDesc, we need the fieldtype array and the fieldname arrays
        	Type[] fieldType = new Type[2];
        	String[] fieldName = new String[2];
        	
        	// set the groupfield type and names for the first field of the new tupledesc
        	fieldType[0] = this._child.getTupleDesc().getFieldType(this._gfield);
            fieldName[0] =  this._child.getTupleDesc().getFieldName(this._gfield);
            
            // set the aggregate field type and name for the second field of the new tupledesc
        	fieldType[1] = this._child.getTupleDesc().getFieldType(_afield);
        	//  "aggName(aop) (child_td.getFieldName(afield))"
        	fieldName[1] = this._op.toString() + 
        				   "(" + this._child.getTupleDesc().getFieldName(this._afield) + ")";
        	return new TupleDesc(fieldType, fieldName);
    	}
    }

    public void close() {
		this.it.close();
    }

    @Override
    public DbIterator[] getChildren() {
    	DbIterator[] result = new DbIterator[1];
    	result[0] = this._child;
    	return result;
    }

    @Override
    public void setChildren(DbIterator[] children) {
//    	result[0] = this._child;
    	this._child = children[0];
    }
    
}
