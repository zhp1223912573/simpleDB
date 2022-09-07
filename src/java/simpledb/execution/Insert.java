package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 * 插入操作，从子运算符的元组中读取有效元组，插入到构造器中的传入的tableId表内
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    //要插入的元组的迭代器
    private OpIterator child;
    //被插入的表的id
    private int tableId;
    //插入操作的事务
    private TransactionId tid;
    //插入的元组描述符
    private TupleDesc tupleDesc;
    //插入的元组数量
    private int numOfInsert;
    //是否已经访问过
    private boolean accessed;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.child = child;
        this.tableId = tableId;
        this.tid = t;
        this.tupleDesc = child.getTupleDesc();
        this.numOfInsert = 0;
        this.accessed = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException{
        // some code goes here
        //


        if(child==null) throw new DbException("插入元组不存在");

        //获取缓冲池
        BufferPool bufferPool = Database.getBufferPool();
        //打开迭代器
        child.open();
        super.open();
        while(child.hasNext()){
            Tuple next = child.next();
            numOfInsert++;
            try{
                bufferPool.insertTuple(tid, tableId,next);
            }catch (IOException e){
                e.printStackTrace();
            }
        }

    }

    public void close() {
        // some code goes here
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     *
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //已经访问过一次 就直接返回null
        if(accessed){
            return null;
        }else{
            accessed = true;
            Type type[] = {Type.INT_TYPE};
            Tuple tuple = new Tuple(new TupleDesc(type));
            tuple.setField(0, new IntField(numOfInsert));
            return tuple;
        }

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
