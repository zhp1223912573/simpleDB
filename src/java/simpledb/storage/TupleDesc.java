package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.lang.reflect.Field;
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

    //元组描述符的条目集合
    private ArrayList<TDItem> TDItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //直接返回该描述符的条目集合的迭代器
        return TDItems.iterator();
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
    public TupleDesc(Type[] typeAr, String[] fieldAr)  {
        // some code goes here
        //保证传入的类型与名称一一对于
        TDItems = new ArrayList<TDItem>(typeAr.length);
        if(typeAr.length>=1&&typeAr.length==fieldAr.length){
            for(int i=0;i<typeAr.length;i++){
                //保证加入的类型都不为null 并且当名称为null时将其设置为noname
                if(typeAr[i]!=null)
                    TDItems.add(new TDItem(typeAr[i],fieldAr[i]!=null?fieldAr[i]:"noname"));
            }
        }
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
        // some code goes here
        TDItems = new ArrayList<TDItem>(typeAr.length);
        if(typeAr.length>=1){
            for(Type type : typeAr)
            TDItems.add(new TDItem(type,"noname"));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItems.size();
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
        //查询坐标大于条目总数
        if(i>TDItems.size()){
            throw new NoSuchElementException("不存在该域");
        }
        TDItem tdItem = TDItems.get(i);
        return tdItem.fieldName;
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
        //查询坐标大于条目总数
        if(i>TDItems.size()){
            throw new NoSuchElementException("不存在该域");
        }
        TDItem tdItem = TDItems.get(i);
        return tdItem.fieldType;
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
        int index =0;
        for(TDItem t:TDItems){
            String realname = t.fieldName;
            if(realname.equals(name)){
                return index;
            }
            index++;
        }

        throw new NoSuchElementException("域名为"+name+"属性不存在！");

    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (TDItem item : this.TDItems) {
            totalSize += item.fieldType.getLen();
        }
        return totalSize;
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

        ArrayList<Type> types = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        for (TDItem item : td1.TDItems) {
            types.add(item.fieldType);
            names.add(item.fieldName);
        }
        for (TDItem item : td2.TDItems) {
            types.add(item.fieldType);
            names.add(item.fieldName);
        }

        Type[] types1 = new Type[types.size()];
        String[] names1 = new String[types.size()];
        types.toArray(types1);
        names.toArray(names1);
        return new TupleDesc(types1, names1);



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
        if(o==null){
            return false;
        }
        try{
            TupleDesc c = (TupleDesc)o;
            //比较两者的数量之后在比较类型是否相同
            if(this.TDItems.size()==c.TDItems.size()){
                for(int i=0;i<TDItems.size();i++){
                    if(!this.TDItems.get(i).fieldName
                            .equals(c.TDItems.get(i).fieldName)){
                        return false;
                    }
                }
            }else{
                return false;
            }
            return true;
        }catch (ClassCastException e){
            e.printStackTrace();
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.toString().hashCode();
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
        String string ="";
        for(TDItem tdItem: TDItems){
           string+=tdItem+",";
        }
        return string.substring(0, string.length()-1);
    }
}
