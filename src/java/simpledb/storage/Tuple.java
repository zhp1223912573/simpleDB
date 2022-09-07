package simpledb.storage;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    //元组包含的数据
    private ArrayList<Field> fields ;
    //元组描述符 形容数据的类型及名称
    private TupleDesc tuple_descriptor;
    //元组id号，用于disk上的定位
    private RecordId recordId;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        //td不为null 并且至少拥有一个field
        if(td!=null&&td.getSize()>=1){
            fields = new ArrayList<>(td.getSize());
           this.tuple_descriptor=td;
       }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(fields, tuple.fields) &&
                Objects.equals(tuple_descriptor, tuple.tuple_descriptor) &&
                Objects.equals(recordId, tuple.recordId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, tuple_descriptor, recordId);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tuple_descriptor;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(fields.size()<i){
            return;
        }
        fields.add(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if(i>fields.size()){
            throw new NoSuchElementException();
        }

        if(fields.get(i)!=null){
            return fields.get(i);
        }
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        //throw new UnsupportedOperationException("Implement this");
        String string="";
        for(Field field : fields){
            string+=field.toString()+"  ";
        }

        return string.substring(0, string.length()-2);
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tuple_descriptor=td;
    }
}
