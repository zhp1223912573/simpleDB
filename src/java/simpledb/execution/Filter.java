package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;
import java.util.prefs.NodeChangeListener;

/**
 * Filter is an operator that implements a relational select.
 * 过滤器 依照断言筛选出合格的元组集合 并保存其输出的迭代器
 * （仿照OrderBy）
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    //断言
    private Predicate predicate;
    //要过滤的表的迭代器
    private OpIterator child;
    //保存过滤后符合断言的元组
    private List<Tuple> childTuples = new ArrayList<>();
    //元组描述符
    private TupleDesc tupleDesc;
    //符合断言的元组集合的迭代器
    private Iterator<Tuple> iterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.child=child;
        this.predicate=p;
        this.tupleDesc=child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        //打开访问的表的迭代器 才能进行迭代
        child.open();
        while(child.hasNext()){
            //获取元组
            Tuple newTuple =child.next();
            //对下一个元组进行断言 为真则加入要输出的元组
            if(predicate.filter(newTuple)){
                childTuples.add(newTuple);
            }
        }
        //将符合条件的所有元组集合的迭代器设置为类内的迭代器
        this.iterator = childTuples.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        iterator = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        // Resets the iterator to the start.
        //恢复输出元组集合的迭代器（将其复原到首个元组）；
        iterator = childTuples.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        //返回通过断言的元组集合中的元组
       if(iterator!=null&&iterator.hasNext()){
           return iterator.next();
       }else{
           return null;
       }

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child=children[0];
    }

}
