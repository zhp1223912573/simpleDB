package simpledb.execution;

import jdk.nashorn.internal.runtime.regexp.joni.constants.OPCode;
import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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
    private  HashMap<Field,Integer> aggregate;
    //记录以分组域为键，总数为value的map结构----用于计算avg
    private HashMap<Field,Integer> count;
    private String groupfieldName="";
    private String fieldName="";


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
        // some code goes here
        this.gbField = gbfield;
        this.gbfieldType= gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.isGroup = (gbfield==Aggregator.NO_GROUPING?false:true);
        aggregate = new HashMap<>();
        count = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //首先根据当前的tup以及isGroup来创建key与value；
        Field key;
        Integer value;
        //分别记录当前情况的聚合值以及总数
        int currentAggreateValue;
        int currentCount;
        fieldName = tup.getTupleDesc().getFieldName(aField);
        //创建key
        if(isGroup){
            key = tup.getField(gbField);
            groupfieldName = tup.getTupleDesc().getFieldName(gbField);
        }else{
            key = new IntField(Aggregator.NO_GROUPING);

        }

        //强转获取value值
        value = ((IntField)tup.getField(aField)).getValue();

        //根据对key是否存在于aggreate HashMap结构中判断是进行初始化
        //若不存在
        if(!aggregate.containsKey(key)){
            //若是Max和Min则需要将该键值的初始value设置为最小或最大值
            if(op==Op.MAX){
                aggregate.put(key, Integer.MIN_VALUE);
            }

            if(op==Op.MIN){
                aggregate.put(key, Integer.MAX_VALUE);
            }

            //剩下三种操作只需要将key对应的value初始化为0
            if(op==Op.SUM||op==Op.AVG||op==Op.COUNT){
                aggregate.put(key, 0);
            }

            //因为该键key是第一次出现说明其出现次数为0 在count中也为0 所以要对在count中的数量进行初始化
            count.put(key, 0);
        }

        //无论是否存在 上述都已经初始化完成 应该保存当前值
        currentAggreateValue = aggregate.get(key);
        currentCount = count.get(key);

        //开始根据操作符进行操作
        if(op==Op.MAX){
            if(currentAggreateValue<value){
                //小于则更新为新合并的元组聚合域
                aggregate.put(key, value);
            }
        }
        if(op==Op.MIN){
            if(currentAggreateValue>value){
                //大于则更新为新合并的元组聚合域
                aggregate.put(key, value);
            }
        }

        if(op==Op.SUM){
            aggregate.put(key, currentAggreateValue+value);
        }

        if(op==Op.COUNT){
            //先自增
            aggregate.put(key,++currentAggreateValue);
        }

        if(op==Op.AVG){

            aggregate.put(key,currentAggreateValue+value);
            //先自增
            count.put(key,++currentCount);
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
                if(this.op== Op.AVG){
                    value /= count.get(key);
                }
                Tuple tuple = new Tuple(td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(value));
                tuples.add(tuple);
            }
        }else{
            //未分组情况下，aggreate以及count结构包含所有元组的聚合元素结果
            Integer value = aggregate.get(new IntField(Aggregator.NO_GROUPING));
            if(op==Op.AVG){
                    value /= count.get(new IntField(Aggregator.NO_GROUPING));
            }
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
