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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 * 删除操作无须传入要删除的元组的tableId，传入的要删除元组迭代器中的元组包含了了这些信息
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    //要删除的元组的迭代器
    private OpIterator child;
    //要删除的元组的描述符
    private TupleDesc tupleDesc;
    //进行删除的事务
    private TransactionId transactionId;
    //是否已经访问过
    private boolean accessed;
    //删除的总数
    private int numOfdelete;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child ) {
        // some code goes here
        this.transactionId = t;
        this.child = child;
        this.tupleDesc = child.getTupleDesc();
        this.accessed = false;
        this.numOfdelete = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();

        BufferPool bufferPool = Database.getBufferPool();
        while(child.hasNext()){
            Tuple next = child.next();
            try {
                bufferPool.deleteTuple(transactionId, next);
                numOfdelete++;
            } catch (IOException e) {
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(accessed) return null;

        accessed = true;
        Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        tuple.setField(0, new IntField(numOfdelete));
        return tuple;
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
