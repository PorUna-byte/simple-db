package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * This class Build an Operator that can emit tuples
 * In the form(groupVal, aggregateVal)
 * The low-level container is a hashtable
 * **/
public class AggregateBuffer extends Operator{
    private final Hashtable<Field, List<Field>> htab;
    private final TupleDesc td;
    private final List<Tuple> tuples=new ArrayList<>();
    private int idx=0;
    public AggregateBuffer(Hashtable<Field, List<Field>> htab, Type gbfieldtype,
                           Type afieldType,Aggregator.Op op,String gbname,String aname){
        this.htab=htab;
        Type[] typeAr;
        String[] nameAr;
        if(gbfieldtype!=null) {
            typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
            nameAr = new String[]{gbname, op.toString()+"("+aname+")"};
        }
        else {
            typeAr = new Type[]{Type.INT_TYPE};
            nameAr=new String[]{aname};
        }
        td = new TupleDesc(typeAr,nameAr);
        if(afieldType==Type.INT_TYPE) {
            for (Field gbval : htab.keySet()) {
                if(htab.get(gbval).size()==0)
                    continue;
                int sum = 0;
                int count = 0;
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                int avg = 0;
                for (Field aval : htab.get(gbval)) {
                    min = Integer.min(min, ((IntField) aval).getValue());
                    max = Integer.max(max, ((IntField) aval).getValue());
                    sum += ((IntField) aval).getValue();
                    count++;
                }
                avg = sum / count;
                Tuple tp = new Tuple(td);
                if(gbfieldtype!=null) {
                    tp.setField(0, gbval);
                    switch (op.toString()) {
                        case "min":
                            tp.setField(1, new IntField(min));
                            break;
                        case "max" :
                            tp.setField(1, new IntField(max));
                            break;
                        case "count":
                            tp.setField(1, new IntField(count));
                            break;
                        case "avg":
                            tp.setField(1, new IntField(avg));
                            break;
                        case "sum":
                            tp.setField(1, new IntField(sum));
                            break;
                    }
                }else{
                    switch (op.toString()) {
                        case "min":
                            tp.setField(0, new IntField(min));
                            break;
                        case "max" :
                            tp.setField(0, new IntField(max));
                            break;
                        case "count":
                            tp.setField(0, new IntField(count));
                            break;
                        case "avg":
                            tp.setField(0, new IntField(avg));
                            break;
                        case "sum":
                            tp.setField(0, new IntField(sum));
                            break;
                    }
                }
                tuples.add(tp);
            }
        }else{
            for (Field gbval : htab.keySet()) {
                int count = htab.get(gbval).size();
                Tuple tp = new Tuple(td);
                if(gbfieldtype!=null) {
                    tp.setField(0, gbval);
                    tp.setField(1, new IntField(count));
                }else
                    tp.setField(0, new IntField(count));
                tuples.add(tp);
            }
        }
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if(idx<tuples.size())
            return tuples.get(idx++);
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[0];
    }

    @Override
    public void setChildren(OpIterator[] children) {
        //we have no children
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        idx=0;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }
}
