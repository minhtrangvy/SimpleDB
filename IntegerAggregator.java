package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int _gbfield;
    private int _afield;
    private Type _gbfieldtype;
    private Op _op;
    
    // keep track of what we have aggregated over and the counts
    private Map<Field, Field> allGroups = new HashMap<Field, Field>();
    
    // we need to create mappings for the count and sum aggregates
    private Map<Field, Integer> countTable = new HashMap<Field, Integer>();
    private Map<Field, Integer> sumTable = new HashMap<Field, Integer>();
    
    // whoop we need a name for the field and we can only get it from the tuple
    // so we need this data member
    private String aggregateFieldName;
    private String groupFieldName;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this._afield = afield;
        this._gbfield = gbfield;
        this._gbfieldtype = gbfieldtype;
        this._op = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple t) {
    	Field groupValueKey;
		IntField none = new IntField(NO_GROUPING);
		this.aggregateFieldName = t.getTupleDesc().getFieldName(this._afield);

    	if (this._gbfield == Aggregator.NO_GROUPING) groupValueKey = none;
    	else {
    		groupValueKey = t.getField(_gbfield);
            groupFieldName = t.getTupleDesc().getFieldName(this._gbfield);
    	}
    	
		IntField currentValue = (IntField) allGroups.get(groupValueKey);
		IntField aggregateValue = (IntField) t.getField(this._afield);

        // for COUNT
    	if (this._op.equals(Op.COUNT)) {
    		int countsofar = 0;
    		
    		// if we are at the first tuple so we make count = 1
    		if (currentValue == null) countsofar = 1;
    		// if there is already a running count, just add to it
    		else countsofar = currentValue.getValue() + 1;
    		
    		// update aggregates so we can update the count!
    		allGroups.put(groupValueKey, new IntField(countsofar));
    	}
    	
    	// for AVG
    	else if (this._op.equals(Op.AVG)) {
    		
    		// we need the count, the sum, and the average
    		int averagesofar;
    		
    		// if the aggregate field doesnt have anything yet, we just set
    		// everything to their initial values
    		if (currentValue == null) {
    			averagesofar = aggregateValue.getValue(); 
    			this.countTable.put(groupValueKey, 1);
    			this.sumTable.put(groupValueKey, aggregateValue.getValue());
    			
    		// else we need to find the new averagesofar
    		// from the current countsofar and sumsofar already stored in the 
    		// sumtable and counttables 
    		} else {
    			int countsofar = countTable.get(groupValueKey) + 1;
    			int sumsofar = sumTable.get(groupValueKey) + aggregateValue.getValue();
    			averagesofar = sumsofar/countsofar;
    			countTable.put(groupValueKey, countsofar);
    			sumTable.put(groupValueKey, sumsofar);
    		}
    		
    		// and then we have an average that we should update the aggregate with
    		allGroups.put(groupValueKey, new IntField(averagesofar));
    	}
    	
    	// for SUM
    	else if (this._op.equals(Op.SUM)) {
    		
    		if (currentValue == null) this.allGroups.put(groupValueKey, new IntField(aggregateValue.getValue()));
    		else this.allGroups.put(groupValueKey, new IntField(aggregateValue.getValue() + currentValue.getValue()));
    	}
    	
    	// for MIN
    	else if (this._op.equals(Op.MIN)) {
    		
    		if (currentValue == null) this.allGroups.put(groupValueKey, new IntField(aggregateValue.getValue()));
    		else this.allGroups.put(groupValueKey, new IntField(Math.min(aggregateValue.getValue(), currentValue.getValue())));
    		
//    		{
//    			// currentValue is the current min
//    			int newMinMaybe = aggregateValue.getValue();
//    			if (newMinMaybe < currentValue.getValue()) this.allGroups.put(groupValueKey, new IntField(newMinMaybe));
//    			else this.allGroups.put(groupValueKey, currentValue);
//    		}
    		
    	}
    	
    	// for MAX
    	else if (this._op.equals(Op.MAX)) {
    		if (currentValue == null) this.allGroups.put(groupValueKey, new IntField(aggregateValue.getValue()));
    		else this.allGroups.put(groupValueKey, new IntField(Math.max(aggregateValue.getValue(), currentValue.getValue())));
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
