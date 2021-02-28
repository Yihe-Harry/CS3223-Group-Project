/**
 * Aggregate function operation
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.LinkedList;

public class Aggregate extends Operator {
    Operator base;      // Base operator
    int batchsize;      // Number of tuples per outbatch
    boolean eos;

    // Stored tuples. This is needed because aggregate is a blocking operation.
    // The aggregate operator scans through the entire underlying operator to get the value when next() is called.
    LinkedList<Tuple> tuples;
    ArrayList<Integer> attrTupleIndex;          // Indexes of aggregated attributes (in tuple)
    ArrayList<Integer> aggregateFunction;       // Aggregate function of attributes
    ArrayList<Integer> projectedAggregateTypes; // Projected aggregate types
    ArrayList<Number> runningAggregates;        // Maintain running aggregates
    int runningCount;
    int inputCursor;

    /**
     * constructor
     **/
    public Aggregate(Operator base, int type,
                     ArrayList<Integer> attrTupleIndex, ArrayList<Attribute> aggregatedAttributes) {
        super(type);
        this.base = base;

        this.tuples = new LinkedList<>();
        this.attrTupleIndex = attrTupleIndex;
        this.aggregateFunction = new ArrayList<>(attrTupleIndex.size());
        this.projectedAggregateTypes = new ArrayList<>(attrTupleIndex.size());

        this.runningAggregates = new ArrayList<>(attrTupleIndex.size());
        this.runningCount = 0;

        if (attrTupleIndex.size() != aggregatedAttributes.size()) {
            System.err.println(
                    "Number of aggregate indexes and number of aggregate types different.\n" +
                            "Check Project.java\n"
            );
            System.exit(1);
        }

        // Checks for invalid operator
        for (int i = 0; i < attrTupleIndex.size(); i++) {
            Attribute curr = aggregatedAttributes.get(i);
            switch (curr.getAggType()) {
                // MIN, MAX, AVG are invalid for Strings
                case Attribute.MIN, Attribute.MAX, Attribute.AVG:
                    if (curr.getProjectedType() == Attribute.STRING) {
                        System.err.println("Invalid attribute for given aggregate function");
                        System.exit(1);
                    }
                default:
            }
            this.projectedAggregateTypes.add(curr.getProjectedType());
            this.aggregateFunction.add(curr.getAggType());
            this.runningAggregates.add(null);
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
        this.eos = false;
        this.inputCursor = 0;
        return base.open();
    }

    private Batch getNextBatch() {
        if (this.tuples.isEmpty()) {
            close();
            return null;
        }

        Batch result = new Batch(batchsize);

        // Add till output batch is full or no more tuples to process
        while (!result.isFull() && !this.tuples.isEmpty()) {
            Tuple tuple = this.tuples.removeFirst();
            ArrayList<Object> current = new ArrayList<>();
            for (Object o : tuple.data()) {
                current.add(o);
            }

            // Append aggregated columns to tuples
            for (int i = 0; i < this.projectedAggregateTypes.size(); i++) {
                int projectedType = this.projectedAggregateTypes.get(i);

                switch (this.aggregateFunction.get(i)) {
                    case Attribute.AVG:
                        if (projectedType == Attribute.REAL) {
                            current.add(this.runningAggregates.get(i).floatValue() / this.runningCount);
                        } else if (projectedType == Attribute.INT) {
                            current.add(this.runningAggregates.get(i).intValue() / this.runningCount);
                        }
                        break;
                    case Attribute.COUNT:
                        current.add(this.runningCount);
                        break;
                    case Attribute.MIN, Attribute.MAX:
                        if (projectedType == Attribute.REAL) {
                            current.add(this.runningAggregates.get(i).floatValue());
                        } else if (projectedType == Attribute.INT) {
                            current.add(this.runningAggregates.get(i).intValue());
                        }
                }
            }
            Tuple outtuple = new Tuple(current);
            result.add(outtuple);
        }
        return result;
    }

    // Scans the entire base operator upon the first call of next, and then releases output in batches
    public Batch next() {
        int i = 0;
        if (eos) {
            return getNextBatch();
        }

        Batch inbatch = this.base.next();

        while (inbatch != null) {
            for (i = inputCursor; i < inbatch.size(); ++i) {
                this.runningCount++;
                Tuple present = inbatch.get(i);

                // Accumulate values for aggregated attributes
                for (int j = 0; j < this.attrTupleIndex.size(); j++) {
                    int aggregateType = this.aggregateFunction.get(j);
                    if (aggregateType == Attribute.COUNT) {
                        continue;
                    }
                    int aggregateIndex = this.attrTupleIndex.get(j);
                    Number value = (Number) present.dataAt(aggregateIndex);
                    Number currentValue = this.runningAggregates.get(j);

                    if (currentValue == null) {
                        this.runningAggregates.set(j, value);
                    } else {

                        switch (aggregateType) {
                            case Attribute.MIN:
                                if (this.projectedAggregateTypes.get(j) == Attribute.INT) {
                                    this.runningAggregates.set(j, Math.min(value.intValue(), currentValue.intValue()));
                                } else {
                                    this.runningAggregates.set(j, Math.min(value.floatValue(), currentValue.floatValue()));
                                }
                                break;
                            case Attribute.MAX:
                                if (this.projectedAggregateTypes.get(j) == Attribute.INT) {
                                    this.runningAggregates.set(j, Math.max(value.intValue(), currentValue.intValue()));
                                } else {
                                    this.runningAggregates.set(j, Math.max(value.floatValue(), currentValue.floatValue()));
                                }
                                break;
                            case Attribute.AVG:
                                if (this.projectedAggregateTypes.get(j) == Attribute.INT) {
                                    this.runningAggregates.set(j, currentValue.intValue() + value.intValue());
                                } else {
                                    this.runningAggregates.set(j, currentValue.floatValue() + value.floatValue());
                                }
                                break;
                        }
                    }
                }
                this.tuples.add(present);
                if (i == inbatch.size())
                    inputCursor = 0;
                else
                    inputCursor = i;
            }
            inbatch = this.base.next();
        }

        eos = true;
        return getNextBatch();
    }

    public boolean close() {
        base.close();
        return true;
    }
}
