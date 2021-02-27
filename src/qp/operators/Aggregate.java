/**
 * Aggregate function operation
 **/

package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.Attribute;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class Aggregate extends Operator {
    private class AttributeDetails {
        public int index, attributeType;
        public Number runningAttribute;
        public int count;

        public AttributeDetails(int index, int attributeType) {
            this.index = index;
            this.attributeType = attributeType;
            this.count = 0;
            this.runningAttribute = null;
        }
    }

    Operator base;      // Base operator
    int batchsize;      // Number of tuples per outbatch
    boolean eos;

    /**
     * The following fields are required during execution of the aggregate operator
     **/
    ArrayList<AttributeDetails> attrs;      // Attributes being aggregated
    ArrayList<Batch> batches;               // Stored batches. This is needed because the aggregate operator scans
                                            // through the entire underlying operator to get the value

    /**
     * constructor
     **/
    public Aggregate(Operator base, int type,
                     ArrayList<Integer> aggregateIndexes, ArrayList<Attribute> aggregatedAttributes) {
        super(type);
        this.base = base;
        this.attrs = new ArrayList<>();

        if (aggregateIndexes.size() != aggregatedAttributes.size()) {
            System.err.println(
                    "Number of aggregate indexes and number of aggregate types different.\n" +
                            "Check Project.java\n"
            );
            System.exit(1);
        }

        // Checks for invalid operator
        for (int i = 0; i < aggregateIndexes.size(); i++) {
            Attribute curr = aggregatedAttributes.get(i);
            switch (curr.getAggType()) {
                case Attribute.MIN, Attribute.MAX, Attribute.AVG:
                    if (curr.getProjectedType() == Attribute.STRING) {
                        System.err.println("Invalid attribute for given aggregate function");
                        System.exit(1);
                    }
                default:
            }
            this.attrs.add(new AttributeDetails(aggregateIndexes.get(i), curr.getAggType()));
        }
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public boolean open() {
        // Select number of tuples per batch
        int tupleSize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tupleSize;
        eos = false;
        return base.open();
    }

    // Scans the entire base operator upon the first call of next, and then releases output in batches
    public Batch next() {
        int i = 0;
        if (eos) {
            close();
            return null;
        }


        // An output buffer is initiated
        outbatch = new Batch(batchsize);

        // keep on checking the incoming pages until the output buffer is full
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = base.next();
                // There is no more incoming pages from base operator
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            // Continue this for loop until this page is fully observed or the output buffer is full
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
                Tuple present = inbatch.get(i);
                this.runningCount++;
                Number value;
                if (this.attr.getProjectedType() == Attribute.INT) {
                    value = (int) present.dataAt(this.index);
                } else {
                    value = (float) present.dataAt(this.index);
                }

                switch (this.aggregateType) {
                    case Attribute.MIN:
                        if (this.attr.getProjectedType() == Attribute.INT) {
                            this.runningAggregate = Math.min(value.intValue(), this.runningAggregate.intValue());
                        } else {
                            this.runningAggregate = Math.min(value.floatValue(), this.runningAggregate.floatValue());
                        }
                        break;
                    case Attribute.MAX:
                        if (this.attr.getProjectedType() == Attribute.INT) {
                            this.runningAggregate = Math.max(value.intValue(), this.runningAggregate.intValue());
                        } else {
                            this.runningAggregate = Math.max(value.floatValue(), this.runningAggregate.floatValue());
                        }
                        break;
                    case Attribute.AVG:
                        if (this.attr.getProjectedType() == Attribute.INT) {
                            this.runningAggregate = this.runningAggregate.intValue() + value.intValue();
                        } else {
                            this.runningAggregate = this.runningAggregate.floatValue() + value.floatValue();
                        }
                        break;
                    default:
                        break;
                }
            }

            // Modify the cursor to the position required when the base operator is called next time;
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }
        return outbatch;
    }

    public boolean close() {
        base.close();
        return true;
    }
}
