package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 * 聚合操作--与join和Filter相似
 *
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    //要进行聚集操作的表的迭代器
    private OpIterator child;
    //聚合域索引
    private  int afield;
    //分组域索引
    private int gbfield;
    //聚合操作
    private Aggregator.Op op;
    //聚合器
    private Aggregator aggregator;
    //传入表描述符
    private TupleDesc tupleDesc;
    //聚合操作后所有元组的迭代器
    private OpIterator iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gbfield = gfield;
        this.op = aop;
        this.tupleDesc = child.getTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gbfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return tupleDesc.getFieldName(gbfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return tupleDesc.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        //初始化聚集器---根据gbfieldId判断
        Type gbfieldType = null;
        if(gbfield!=Aggregator.NO_GROUPING){
            gbfieldType = tupleDesc.getFieldType(gbfield);
        }
        //如果要进行聚集的域为int类型创建对应聚集器
        if(tupleDesc.getFieldType(afield)==Type.INT_TYPE){
            this.aggregator = new IntegerAggregator(gbfield,gbfieldType,afield ,op );
        }else{
            this.aggregator = new StringAggregator(gbfield, gbfieldType, afield, op);
        }
        //开启子表迭代器，遍历所有元组
        child.open();
        while(child.hasNext()){
            //合并
            aggregator.mergeTupleIntoGroup(child.next());
        }
        //合并后获取迭代器
        this.iterator = aggregator.iterator();
        //分别打开迭代器
        iterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(iterator!=null&&iterator.hasNext()){
            return iterator.next();
        }else{
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return iterator.getTupleDesc();
    }

    public void close() {
        // some code goes here
        child.close();
        iterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
