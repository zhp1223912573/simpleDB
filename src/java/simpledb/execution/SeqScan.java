package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    //进行扫描的事务操作的id
    private TransactionId tid;
    //扫描的表id
    private int tableId;
    //扫描的表的别名
    private String tableAlias;
    //表的迭代器
    private DbFileIterator tableIterator;

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid=tid;
        this.tableId=tableid;
        this.tableAlias=tableAlias;
        this.tableIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableAlias=tableAlias;
        this.tableId=tableid;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        tableIterator.open();

    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     *         返回一个描述符，该描述符所描述的表若需要将其域名重新修改，添加一个在
     *         此次扫描时加入的表别名，改为"别名.域名"的形式，当别名不存在时，改成
     *         "null.域名"
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //获取需要修改的描述符
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        int nums = tupleDesc.numFields();
        //重新补充创建一个新的描述所需要的数组
        Type[] typeAr = new Type[nums];
        String[] fieldAr = new String[nums];
        for(int i=0;i<nums;i++){
            typeAr[i] = tupleDesc.getFieldType(i);
            String oldName = tupleDesc.getFieldName(i);
            fieldAr[i]=(getAlias()==null?"null.":getAlias()+".")+oldName;
        }
        return new TupleDesc(typeAr,fieldAr);

    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here

        return tableIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return tableIterator.next();
    }

    public void close() {
        // some code goes here
        tableIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        tableIterator.rewind();
    }
}
