package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {
	
	public JoinPredicate _predicate;
	public DbIterator _child1;
	public DbIterator _child2;
	
	boolean hasNext = true;
	boolean needToRead = true;
	Tuple outerTuple = null;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
//    	System.out.println("in join constructor");
        this._predicate = p;
        this._child1 = child1;
        this._child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return this._predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return this._child1.getTupleDesc().getFieldName(_predicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return this._child2.getTupleDesc().getFieldName(_predicate.getField1());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDescChild1 = this._child1.getTupleDesc();
        TupleDesc tupleDescChild2 = this._child2.getTupleDesc();
        TupleDesc merged = TupleDesc.merge(tupleDescChild1, tupleDescChild2);
        return merged;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
//    	System.out.println("in join open");
        this._child1.open();
        this._child2.open();
        super.open();
    }

    public void close() {
        this._child1.close();
        this._child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this._child1.rewind();
        this._child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * f
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	
    	// First time we run the program, we need to get the first tuple
    	if (needToRead) {
    		// so we check if child1 has a next
    		if (this._child1.hasNext()) {
    			hasNext = true;
    		}
    	}
//    	System.out.println("in fetchNext: hasNext is " + hasNext + " and needToRead is " + needToRead);
    	
    	    	
    	// while there are more tuples in the outer relation
    	while (hasNext) {
    	    
    		// if we need to read this outer tuple
    		if (needToRead) {
    			// we call next to get the actual first tuple
    			outerTuple = this._child1.next();
//        	    System.out.println("OUTER tuple is " + outerTuple.toString());
    		}

        	// while there are more tuples in the inner relation
    		while (this._child2.hasNext()) {

    			// grab the next inner tuple 
    			Tuple innerTuple = this._child2.next();
//        	    System.out.println("inner tuple is " + innerTuple.toString());

    			// check if the tuples satisfy the join condition
    			if (this._predicate.filter(outerTuple, innerTuple)) {
    				
    				Tuple newTuple = new Tuple(this.getTupleDesc());
    				
    				// join tuples
    				int index = 0;
    				for (int i = 0; i < outerTuple.getTupleDesc().numFields(); i ++) {
    					newTuple.setField(i, outerTuple.getField(i));
    					index++;
    				}
    				for (int j = 0; j < innerTuple.getTupleDesc().numFields(); j ++ ) {
    					newTuple.setField(index, innerTuple.getField(j));
    					index++;
    				}
    				
    				// we just read the current tuple combination, no need to do it again
    				needToRead = false;
    				
    				return newTuple;
    			}
    		}
    		// rewind inner tuple iterator and rest variables
//        	System.out.println("in fetchNext END whileloop");

    		this._child2.rewind();
    		hasNext = this._child1.hasNext();
    		needToRead = true;
    	}
//    	System.out.println("in fetchNext END");

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = new DbIterator[2];
        children[0] = this._child1;
        children[1] = this._child2;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this._child1 = children[0];
        this._child2 = children[1];
    }

}