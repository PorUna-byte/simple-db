package simpledb.optimizer;
import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram{
    private final int buckets;
    private final int min;
    private final int max;
    private int total;
    private final int[] histogram;
    private int width;
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
        this.buckets = Math.min(max-min+1,buckets);
        this.min     = min;
        this.max     = max;
        this.total   = 0;
        this.histogram = new int[this.buckets];
        this.width = (max+1-min)/this.buckets ;
        for(int i=0;i<this.buckets;i++)
            histogram[i]=0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v>max||v<min)
            return;
        total++;

        int idx=Math.min((int)(((float)(v-min)*buckets)/((float)max-(float)min+1)),buckets-1);
        if(idx==-1){
            System.out.println("idx="+idx+", min="+min+",max="+max+"v="+v);
        }
        histogram[idx]++;
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
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
//        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, NOT_EQUALS;
        int idx=Math.min((int)(((float)(v-min)*buckets)/((float)max-(float)min+1)),buckets-1);
        if(v>max){
            if(op==Predicate.Op.LESS_THAN||op== Predicate.Op.LESS_THAN_OR_EQ||op== Predicate.Op.NOT_EQUALS)
                return 1.0;
            else
                return 0.0;
        }else if(v<min){
            if(op==Predicate.Op.LESS_THAN||op== Predicate.Op.LESS_THAN_OR_EQ||op== Predicate.Op.EQUALS)
                return 0.0;
            else
                return 1.0;
        }
        float sum=0;
        switch (op){
            case EQUALS:
                return ((float)histogram[idx]/width)/total;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                //The right boarder of current histogram
                int rightb=min+(idx+1)*width-1;
                sum = (float)(rightb-v)/width*histogram[idx];
                //Add all right histogram
                for(int i=idx+1;i<buckets;i++)
                    sum+=histogram[i];
                //deal with equal case
                sum += (op== Predicate.Op.GREATER_THAN_OR_EQ)?(float)(histogram[idx]/width):0;
                return sum/total;
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                //The left boarder of current histogram
                int leftb=min+idx*width;
                sum = (float)(v-leftb)/width*histogram[idx];
                //Add all left histogram
                for(int i=0;i<idx;i++)
                    sum+=histogram[i];
                //deal with equal case
                sum += (op== Predicate.Op.LESS_THAN_OR_EQ)?(float)(histogram[idx]/width):0;
                return sum/total;
            case NOT_EQUALS:
                return 1-((float)histogram[idx]/width)/total;
        }
    	// some code goes here
        return -1.0;
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
        StringBuilder sb=new StringBuilder();
        for(int i=0;i<buckets;i++){
            sb.append("[").append(min + i * width).append(",").
                    append(min + ((i + 1) * width)).append(")-->").append((float) histogram[i] / total).append("\n");
        }
        return sb.toString();
    }
}
