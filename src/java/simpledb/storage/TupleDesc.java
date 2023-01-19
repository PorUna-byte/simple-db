package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {
        private static final long serialVersionUID = 1L;
        /**
         * The type of the field
         * */
        public final Type fieldType;
        /**
         * The name of the field
         * */
        public final String fieldName;
        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    public List<TDItem>       items  = new ArrayList<TDItem>();
    public int                length = 0;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        assert typeAr.length>0;
        for(int i=0;i<typeAr.length;i++)
            items.add(new TDItem(typeAr[i],fieldAr[i]));
        length = items.size();
        // some code goes here
    }
    public TupleDesc(List<TDItem> items,String prefix){
        assert items.size()>0;
        for(int i=0;i<items.size();i++)
            this.items.add(new TDItem(items.get(i).fieldType,prefix+"."+items.get(i).fieldName));
        length = this.items.size();
    }
    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        assert typeAr.length>0;
        for (Type type : typeAr) items.add(new TDItem(type, ""));
        length = items.size();
        // some code goes here
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=length)
            throw new NoSuchElementException();
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=length)
            throw new NoSuchElementException();
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i=0;i<length;i++){
            if(items.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int total = 0;
        for(int i=0;i<length;i++)
            total += items.get(i).fieldType.getLen();
        return total;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();
        for(TDItem item:td1.items){
            typeList.add(item.fieldType);
            nameList.add(item.fieldName);
        }
        for(TDItem item:td2.items){
            typeList.add(item.fieldType);
            nameList.add(item.fieldName);
        }
        Type[] typeAr = typeList.toArray(new Type[0]);
        String[] nameAr = nameList.toArray(new String[0]);
        return new TupleDesc(typeAr,nameAr);
    }
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2,Set<Integer> join_fields) {
        // some code goes here
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();
        for(TDItem item:td1.items){
            typeList.add(item.fieldType);
            nameList.add(item.fieldName);
        }
        for(int i=0;i<td2.items.size();i++){
            if(!join_fields.contains(i)) {
                typeList.add(td2.items.get(i).fieldType);
                nameList.add(td2.items.get(i).fieldName);
            }
        }
        Type[] typeAr = typeList.toArray(new Type[0]);
        String[] nameAr = nameList.toArray(new String[0]);
        return new TupleDesc(typeAr,nameAr);
    }
    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof TupleDesc))
            return false;
        TupleDesc other = (TupleDesc)o;
        if(length!=other.length)
            return false;
        for(int i=0;i<length;i++)
            if(!getFieldType(i).equals(other.getFieldType(i)))
                return false;
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for(TDItem item:items){
            sb.append(item.fieldType);
            sb.append("(");
            sb.append(item.fieldName);
            sb.append("),");
        }
        String str=sb.toString();
        return str.substring(0,str.length()-1);
    }
    /**
     * This method change all fieldName to be prefix.fieldName and return a new TupleDesc
     */
    public TupleDesc SetPrefix(String prefix){
        return new TupleDesc(this.items,prefix);
    }
}
