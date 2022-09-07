package simpledb.optimizer;

import junit.extensions.TestSetup;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.IntegerAggregator;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.jnlp.IntegrationService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    //表示的表的id
    private int tableId;
    //表示的表的页数
    private int pageNum;
    //读取每页的io消耗
    private int ioCostPage;
    //总元组数
    private int totalTup;
    //每个域的直方图
    private HashMap<Integer,Object> fieldToHist;
    //每个域的最大值最小值
    private HashMap<Integer,Integer> max;
    private HashMap<Integer,Integer> min;
    private final TupleDesc tupleDesc;


    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage)  {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        HeapFile databaseFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.tableId = tableid;
        this.pageNum = databaseFile.numPages();
        this.ioCostPage = ioCostPerPage;
         this.tupleDesc = databaseFile.getTupleDesc();
         this.totalTup=0;
         this.max = new HashMap<>();
         this.min = new HashMap<>();
         this.fieldToHist = new HashMap<>();

        /*对整个表进行两次遍历，第一次遍历得到最大值最小值，第二次遍历往直方图中添加域的所有值*/

        try{
            DbFileIterator iterator = databaseFile.iterator(new TransactionId());
            iterator.open();
            //第一次遍历，获取各个域的所有最大值最小值
            while(iterator.hasNext()){
                totalTup++;
                Tuple next = iterator.next();
                int numFields = tupleDesc.numFields();
                for(int i=0;i<numFields;i++){
                    //遍历每个元组的所有域 如果是Int类型 则进行统计 无须对String类型进行统计
                    //因为在String类型的直方图中包含了对最大值最小值的查找与处理
                    if(tupleDesc.getFieldType(i)==Type.INT_TYPE){
                        //获取当前元组的值
                        Integer value = ((IntField) next.getField(i)).getValue();
                        //获取最大最小值
                        if(!max.containsKey(i)){
                            max.put(i,value);
                        }else{
                            if(max.get(i)<value){
                                max.put(i, value);
                            }
                        }
                        if(!min.containsKey(i)){
                            min.put(i,value);
                        }else{
                            if(min.get(i)>value){
                                min.put(i, value);
                            }
                        }

                    }

                }

            }

            //对每个域创造其直方图
            for(int i=0;i<tupleDesc.numFields();i++){
                if(tupleDesc.getFieldType(i)==Type.INT_TYPE){
                    IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, min.get(i), max.get(i));
                    fieldToHist.put(i,intHistogram);
                }
                else if(tupleDesc.getFieldType(i)==Type.STRING_TYPE){
                    StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                    fieldToHist.put(i,stringHistogram);
                }
            }

            iterator.rewind();
            //第二次遍历，往直方图中添加数据
            while(iterator.hasNext()){
                Tuple next = iterator.next();
                int numFields = tupleDesc.numFields();
                for(int i=0;i<numFields;i++){
                    if(tupleDesc.getFieldType(i)==Type.INT_TYPE){
                        //要加入的值
                        Integer value=((IntField) next.getField(i)).getValue();
                        ((IntHistogram)fieldToHist.get(i)).addValue(value);
                    }else if(tupleDesc.getFieldType(i)==Type.STRING_TYPE){
                        String value=((StringField)next.getField(i)).getValue();
                        ((StringHistogram)fieldToHist.get(i)).addValue(value);
                    }
                }

            }

        }catch (DbException e){
            e.printStackTrace();
        }catch (TransactionAbortedException e){
            e.printStackTrace();
        }


    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 忽视缓存和寻道时间，读取的页不论包含多少个元组，都按读取一页的开销计算
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.pageNum*this.ioCostPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     *
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        //先判断选择的域是什么类型，在调用相应类型的直方图

        if(tupleDesc.getFieldType(field)==Type.INT_TYPE){
            Integer v =((IntField)constant).getValue();
            return ((IntHistogram)fieldToHist.get(field)).estimateSelectivity(op,v);
        }else if(tupleDesc.getFieldType(field)==Type.STRING_TYPE){
            String v =((StringField)constant).getValue();
            return ((StringHistogram)fieldToHist.get(field)).estimateSelectivity(op,v);
        }
        return -1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return totalTup;
    }

}
