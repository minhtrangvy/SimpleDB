package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private String aggregateFieldName;
    private String groupFieldName;
    private Map<Field, Field> allGroups = new HashMap<Field, Field>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if (!what.equals(Op.COUNT)) throw new IllegalArgumentException("what can only be COUNT!");
        this._afield = afield;
        this._gbfield = gbfield;
        this._gbfieldtype = gbfieldtype;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field groupValueKey;
		IntField none = new IntField(NO_GROUPING);
		this.aggregateFieldName = tup.getTupleDesc().getFieldName(this._afield);
		
    	if (this._gbfield == Aggregator.NO_GROUPING) groupValueKey = none;
    	else {
    		groupValueKey = tup.getField(_gbfield);
            this.groupFieldName = tup.getTupleDesc().getFieldName(this._gbfield);
    	}
    	
		IntField currentValue = (IntField) allGroups.get(groupValueKey);
    	int countsofar; 
    	
		// if we are at the first tuple so we make count = 1
		if (currentValue == null) countsofar = 1;
		// if there is already a running count, just add to it
		else countsofar = currentValue.getValue() + 1;
		
		// update aggregates so we can update the count!
		allGroups.put(groupValueKey, new IntField(countsofar));
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	
    	// need a list in order for us to iterate
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
      
        // we want to return a tuple of the form (aggregateValue)                
        if(this._gbfield == NO_GROUPING){
                
        		// we need to create a tuple desc with one field
                Type[] fieldtypes = new Type[1];
                fieldtypes[0] = Type.INT_TYPE;
                String[] fieldnames = new String[1];
                fieldnames[0] = this.aggregateFieldName;
                TupleDesc currTupDesc = new TupleDesc(fieldtypes, fieldnames);
                
                // create a new tuple and set the first field to NO GROUPING 
                Tuple tup = new Tuple(currTupDesc);
                tup.setField(0, this.allGroups.get(new IntField(NO_GROUPING)));
                
                // add the tuple to the list
                // and then return the tuple iterator to the list
                tuples.add(tup);
                return new TupleIterator(currTupDesc, tuples);
                
        } else {
                
                // we want a tuple desc with two fields
	            Type[] fieldtypes = new Type[2];
	            fieldtypes[0] = this._gbfieldtype;
	            fieldtypes[1] = Type.INT_TYPE;
	            String[] fieldnames = new String[2];
	            fieldnames[0] = this.groupFieldName;
	            fieldnames[1] = this.aggregateFieldName;
	            TupleDesc currTupDesc = new TupleDesc(fieldtypes, fieldnames);
            
	            // get all the groups so we can iterate over them 
	            // hashmaps have a keyset method that returns all the keys (groups) in a set! so useful
                Set<Field> groupssofar = this.allGroups.keySet();
                
                // create new tuples and set values of groups and aggregates
                // by going over all the groups in the list of things we have aggreagated over
                for (Field currentfield : groupssofar){
                        Tuple newTuple = new Tuple(currTupDesc);
                        newTuple.setField(0, currentfield);
                        newTuple.setField(1, this.allGroups.get(currentfield));
                        tuples.add(newTuple);
                }
                                
                return new TupleIterator(currTupDesc, tuples);
                
        }
    }

}
