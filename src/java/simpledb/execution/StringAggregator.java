package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 * That is,the aggregate field is string
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;
    private final IntField sentinel = new IntField(-1);
    private String gbname;
    private String aname;
    //This is a hashtable that maps a group-by field to a list of aggregate field
    private Hashtable<Field, List<Field>> htab=new Hashtable<>();
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException{
        if(what!=Op.COUNT)
            throw new IllegalArgumentException();
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.op=what;
        if(gbfield==NO_GROUPING){
            htab.put(sentinel,new ArrayList<>());
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        aname=tup.getTupleDesc().getFieldName(afield);
        if(gbfield!=NO_GROUPING) {
            gbname=tup.getTupleDesc().getFieldName(gbfield);
            if (!htab.containsKey(tup.getField(gbfield))) {
                htab.put(tup.getField(gbfield), new ArrayList<>());
            }
            htab.get(tup.getField(gbfield)).add(tup.getField(afield));
        }else{
            htab.get(sentinel).add(tup.getField(afield));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggregateBuffer(htab,gbfieldtype,Type.STRING_TYPE,op,gbname,aname);
    }

}
