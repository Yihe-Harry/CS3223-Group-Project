/**
 * Aggregate function operation
 **/

package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.Attribute;

public class Aggregate extends Operator {

    public static final int MIN = 0;
    public static final int MAX = 1;
    public static final int AVG = 2;
    public static final int CNT = 3;

    Operator base;      // Base operator
    int batchsize;      // Number of tuples per outbatch

    int aggregateType;  // Aggregate type
    Attribute attr;     // Attribute being aggregated
    int index;          // Index of attribute being aggregated in table

    int runningCount;           // Running count of tuples processed
    Object runningAggregate;    // Running aggregate value of tuples processed

    /**
     * The following fields are required during execution of the aggregate operator
     **/
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer

    /**
     * constructor
     **/
    public Aggregate(Operator base, int type, int aggregateType, Attribute attr) {
        super(type);
        this.base = base;
        this.aggregateType = aggregateType;
        this.attr = attr;
        this.count = 0;
    }

    public boolean open() {
        // Select number of tuples per batch
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        this.start = 0;
        this.eos = false;
        this.index = schema.indexOf(this.attr);

        return base.open();
    }
    
    public Batch next() {
        int i = 0;
        if (eos) {
            close();
            return null;
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        // keep on checking the incoming pages until the output buffer is full
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = base.next();
                /** There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            /** Continue this for loop until this page is fully observed
             ** or the output buffer is full
             **/
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
                Tuple present = inbatch.get(i);
                this.count++;
                Object value = present.dataAt(this.index);
                switch (this.aggregateType) {
                    case Aggregate.MIN:
                        this.runningAggregate = min(value, this.runningAggregate);
                    case Aggregate.MAX:
                        this.runningAggregate = max(value, this.runningAggregate);
                    case Aggregate.AVG:
                        this.runningAggregate += value;
                    default:
                        continue;
                }
            }

            /** Modify the cursor to the position requierd
             ** when the base operator is called next time;
             **/
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }
        return outbatch;
    }

    public boolean close() {

    }
}
