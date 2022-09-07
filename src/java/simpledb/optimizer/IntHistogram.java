package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.text.DecimalFormat;

/** A class to represent a fixed-width histogram over a single integer-based field.
 * 整型直方图 通过表数据进行选择性估计，按照讲义中的桶基础进行计算。
 *
 */
public class IntHistogram {
    //桶的数量
    private int buckets;
    //最小值
    private int min;
    //最大值
    private int max;
    //范围
    private int width;
    //桶
    private int[] intHistogram;
    //直方图的记录条数
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        double range = (double)(max-min+1)/buckets;
        this.width = (int) Math.ceil(range);
        this.intHistogram= new int[buckets];
        this.ntups = 0;

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        //将值value划分到指定的桶内

        int index = valueToIndex(v);

        if(v==max) intHistogram[index]++;
        else{
            intHistogram[index]++;
        }
        ntups++;
    }

    private int valueToIndex(int v) {
        if (v == max) {
            return buckets - 1;
        } else {
            return (v - min) / width;
        }
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     * 讲义：
     * To estimate the selectivity of a range expression f>const,
     * compute the bucket b that const is in, with width w_b and height h_b.
     * Then, b contains a fraction b_f = h_b / ntups of the total tuples.
     * Assuming tuples are uniformly distributed throughout b,
     * the fraction b_part of b that is > const is (b_right - const) / w_b,
     * where b_right is the right endpoint of b's bucket. Thus, bucket b contributes (b_f x b_part) selectivity to the predicate.
     * In addition, buckets b+1...NumB-1 contribute all of their selectivity
     * (which can be computed using a formula similar to b_f above).
     * Summing the selectivity contributions of all the buckets will yield the overall selectivity of the expression.
     * 计算选择性
     * 首先要获取常数值v所在的桶b的位置，计算出w_b(范围）与h_b(数量）
     * 从而计算出b在整个直方图中的比例b_f=h_b/ntups(总数量）
     * 假设元组在直方图中均匀分布，获取（b_right（范围右端点）-常数值v）/w_b
     * 在通过b_f*b_part计算出选择性，同时累加后面的桶也贡献了的选择性
     */
    public double estimateSelectivity(Predicate.Op op, int v) {


        int bucketPos = valueToIndex(v);    //桶所在的位置
        int right = bucketPos*width+min+width-1;    //桶所在位置的右端点
        //下述代码应该放在各种情况下的里面 不然会有超出界限的异常
//        int height = intHistogram[bucketPos];   //该桶总数
        double selectivity = 0.0;
        int height =0;

    	// some code goes here
        switch (op){


            case EQUALS:

                if(v<min||v>max) return 0.0;
                 height = intHistogram[bucketPos];   //该桶总数
                selectivity = (1.0/width)*(1.0*height/ntups);
            break;

            case GREATER_THAN:

            if(v>max){
                return 0.0;
            }else if(v<=min){
                return 1.0;
            }
             height = intHistogram[bucketPos];   //该桶总数
            selectivity =((right-v)/width)*(height/ntups);
            //还有大于右边的所有值
            double sum =0.0;
            for(int i=bucketPos+1;i<buckets;i++){
                sum+=intHistogram[i];
            }
            selectivity+=(sum/ntups);
                break;

            case LESS_THAN:
           selectivity = 1.0-estimateSelectivity(Predicate.Op.GREATER_THAN, v);
                break;

            case LESS_THAN_OR_EQ:

            selectivity = estimateSelectivity(Predicate.Op.LESS_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS, v);
                break;

            case GREATER_THAN_OR_EQ:

            selectivity = estimateSelectivity(Predicate.Op.GREATER_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS, v);
                break;

            case LIKE:

            selectivity = avgSelectivity();
                break;

            case NOT_EQUALS:

            selectivity = 1-estimateSelectivity(Predicate.Op.EQUALS, v);
                break;

        }
//        DecimalFormat df = new DecimalFormat( "0.0 ");
//        String format = df.format(selectivity);
//        selectivity = Double.parseDouble(format);
        return selectivity>1.0?1.0:selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
