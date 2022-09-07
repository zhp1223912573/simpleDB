package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 * 主构造函数中说明了StringAggreator只支持COUNT的聚集操作
 * 其他步骤的实现基本于IntegerAggregator中相同
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //按该索引所在的域进行分组
    private int gbField;
    //索引所在域的分组类型
    private Type gbfieldType;
    //聚集域的索引
    private int aField;
    //聚集操作
    private Aggregator.Op op;
    //聚合函数是否有分组
    private boolean isGroup ;
    //记录以分组域为键，聚合值为value的map结构
    private HashMap<Field,Integer> aggregate;

    private String groupfieldName="";
    private String fieldName="";
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what!=Op.COUNT)
            throw new IllegalArgumentException("StringAggregator不支持处理CUOUNT外的其他聚合函数操作");
        this.gbField = gbfield;
        this.gbfieldType= gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.isGroup = (gbfield==Aggregator.NO_GROUPING?false:true);
        aggregate = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key;
        //分别记录当前情况的聚合值以及总数
        int currentAggreateValue;
        fieldName = tup.getTupleDesc().getFieldName(aField);
        //创建key
        if(isGroup){
            key = tup.getField(gbField);
            groupfieldName = tup.getTupleDesc().getFieldName(gbField);
        }else{
            key = new IntField(Aggregator.NO_GROUPING);

        }

        /*该段代码错误
        因为此时传入的聚集域是string类型 如果强转 就会导致类型错误
        而且在StringAggregator内不需要关心除了count之外的聚集操作，
        所以此处只需获取key只并增长value值即可*/
        //强转获取value值
        //value = ((IntField)tup.getField(aField)).getValue();

        //根据对key是否存在于aggreate HashMap结构中判断是进行初始化
        //若不存在
        if(!aggregate.containsKey(key)){

            if(op==Op.COUNT){
                aggregate.put(key, 0);
            }

        }

        //无论是否存在 上述都已经初始化完成 应该保存当前值
        currentAggreateValue = aggregate.get(key);

        if(op==Op.COUNT){
            //先自增
            aggregate.put(key,++currentAggreateValue);
        }

    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td =getTupleDesc();
        //分组与否来进行设置
        if(isGroup){
            //分组情况下 遍历键集，若求解的是平均值则对value值重新处理
            for(Field key : aggregate.keySet()){
                Integer value = aggregate.get(key);
                Tuple tuple = new Tuple(td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(value));
                tuples.add(tuple);
            }
        }else{
            //未分组情况下，aggreate以及count结构包含所有元组的聚合元素结果
            Integer value = aggregate.get(new IntField(Aggregator.NO_GROUPING));

            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(value));
            tuples.add(tuple);
        }

        return new TupleIterator(td, tuples);
    }

    /**
     * 获取要返回的元组描述符
     * @return
     */
    private TupleDesc getTupleDesc() {
        Type typeAr[] ;
        String nameAr[];
        if(isGroup){
            typeAr = new Type[2];
            typeAr[0]=gbfieldType;
            typeAr[1]=Type.INT_TYPE;
            nameAr = new String[2];
            nameAr[0]=groupfieldName;
            nameAr[1]=fieldName;
        }else{
            typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            nameAr = new String[1];
            nameAr[0] =fieldName;
        }
        return new TupleDesc(typeAr,nameAr
        );
    }
}
