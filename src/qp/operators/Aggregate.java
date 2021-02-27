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

    // Stored tuples. This is needed because the aggregate operator scans through the entire underlying operator to get the value
    LinkedList<Tuple> tuples;
    ArrayList<Integer> aggregateIndexes;
    ArrayList<Integer> aggregateFunction;
    ArrayList<Integer> projectedAggregateTypes;
    ArrayList<Number> runningAggregates;
    int runningCount;
    int inputCursor;

    /**
     * constructor
     **/
    public Aggregate(Operator base, int type,
                     ArrayList<Integer> aggregateIndexes, ArrayList<Attribute> aggregatedAttributes) {
        super(type);
        this.base = base;
        this.tuples = new LinkedList<>();
        this.aggregateFunction = new ArrayList<>();
        this.runningAggregates = new ArrayList<>(aggregateIndexes.size());
        this.aggregateIndexes = new ArrayList<>(aggregateIndexes);
        this.projectedAggregateTypes = new ArrayList<>();
        this.runningCount = 0;

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
        eos = false;
        inputCursor = 0;
        return base.open();
    }

    private Batch getNextBatch() {
        if (this.tuples.isEmpty()) {
            close();
            return null;
        }

        Batch result = new Batch(batchsize);

        while (!result.isFull() && !this.tuples.isEmpty()) {
            Tuple tuple = this.tuples.removeFirst();
            ArrayList<Object> current = new ArrayList<>();

            for (int j = 0; j < tuple.size(); j++) {
                if (this.aggregateIndexes.contains(j)) {
                    int projectedType = this.projectedAggregateTypes.get(j);
                    switch (this.aggregateFunction.get(j)) {
                        case Attribute.AVG:
                            if (projectedType == Attribute.REAL) {
                                current.add(this.runningAggregates.get(j).floatValue() / this.runningCount);
                            } else if (projectedType == Attribute.INT) {
                                current.add(this.runningAggregates.get(j).intValue() / this.runningCount);
                            }
                            break;
                        case Attribute.COUNT:
                            current.add(this.runningCount);
                            break;
                        default:
                            if (projectedType == Attribute.REAL) {
                                current.add(this.runningAggregates.get(j).floatValue());
                            } else if (projectedType == Attribute.INT) {
                                current.add(this.runningAggregates.get(j).intValue());
                            }
                    }
                } else {
                    Object data = tuple.dataAt(j);
                    current.add(data);
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

                for (int j = 0; j < this.aggregateIndexes.size(); j++) {
                    int aggregateType = this.aggregateFunction.get(j);
                    int aggregateIndex = this.aggregateIndexes.get(j);

                    // If String, must be COUNT function
                    if (this.projectedAggregateTypes.get(j) == Attribute.STRING) {
                        continue;
                    }
                    Number value = (Number) present.dataAt(aggregateIndex);
                    Number currentValue = this.runningAggregates.get(aggregateIndex);

                    if (currentValue == null) {
                        this.runningAggregates.set(j, value);
                    } else {

                        switch (aggregateType) {
                            case Attribute.MIN:
                                if (this.aggregateFunction.get(j) == Attribute.INT) {
                                    this.runningAggregates.set(j, Math.min(value.intValue(), currentValue.intValue()));
                                } else {
                                    this.runningAggregates.set(j, Math.min(value.floatValue(), currentValue.floatValue()));
                                }
                                break;
                            case Attribute.MAX:
                                if (this.aggregateFunction.get(j) == Attribute.INT) {
                                    this.runningAggregates.set(j, Math.max(value.intValue(), currentValue.intValue()));
                                } else {
                                    this.runningAggregates.set(j, Math.max(value.floatValue(), currentValue.floatValue()));
                                }
                                break;
                            case Attribute.AVG:
                                if (this.aggregateFunction.get(j) == Attribute.INT) {
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
