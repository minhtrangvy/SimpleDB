package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
	public Predicate _predicate;
	public DbIterator _iterator;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this._predicate = p;
        this._iterator = child;
    }

    public Predicate getPredicate() {
        return this._predicate;
    }

    public TupleDesc getTupleDesc() {
        return this._iterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this._iterator.open();
        super.open();
    }

    public void close() {
        this._iterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this._iterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        
        while (this._iterator.hasNext()) {
        	Tuple next = this._iterator.next();
        	if (this._predicate.filter(next)) {
        		return next;
        	}
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
       return new DbIterator[] { this._iterator };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (this._iterator != children[0]) {
        	this._iterator = children[0];
        }
    }

}
