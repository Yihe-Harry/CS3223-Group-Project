/**
 * Aggregate function operation
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.LinkedList;

public class Aggregate extends Project {
    boolean eos;

    // Stored tuples. This is needed because the aggregate operator scans through the entire
    // underlying operator to get the value when next() is called.
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
    public Aggregate(Operator base, ArrayList<Attribute> as, int type) {
        super(base, as, type);
        this.tuples = new LinkedList<>();
        this.attrTupleIndex = new ArrayList<>();
        this.aggregateFunction = new ArrayList<>();
        this.projectedAggregateTypes = new ArrayList<>();
        this.runningAggregates = new ArrayList<>();
        this.runningCount = 0;
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
        if (!base.open()) return false;
        this.eos = false;
        this.inputCursor = 0;
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];

        for (int i = 0; i < attrset.size(); i++) {
            Attribute attr = attrset.get(i);

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;

            // Is an aggregated attribute
            if (attr.getAggType() != Attribute.NONE) {
                attr.setType(baseSchema.getAttribute(index).getType());
                this.projectedAggregateTypes.add(attr.getProjectedType());
                this.aggregateFunction.add(attr.getAggType());
                this.runningAggregates.add(null);
                this.attrTupleIndex.add(index);
            }
        }
        return true;
    }

    private Batch getNextBatch() {
        if (this.tuples.isEmpty()) {
            close();
            return null;
        }

        Batch result = new Batch(batchsize);
        int aggregateCount = 0;

        // Add till output batch is full or no more tuples to process
        while (!result.isFull() && !this.tuples.isEmpty()) {
            Tuple tuple = this.tuples.removeFirst();
            ArrayList<Object> current = new ArrayList<>();

            // Accumulate all attributes into tuple
            for (int i = 0; i < attrset.size(); i++) {
                Attribute attr = attrset.get(i);

                if (attr.getAggType() == Attribute.NONE) {
                    current.add(tuple.dataAt(attrIndex[i]));
                } else {
                    int projectedType = this.projectedAggregateTypes.get(aggregateCount);

                    switch (this.aggregateFunction.get(aggregateCount)) {
                        case Attribute.AVG:
                            if (projectedType == Attribute.REAL) {
                                current.add(this.runningAggregates.get(aggregateCount).floatValue() / this.runningCount);
                            } else if (projectedType == Attribute.INT) {
                                current.add(this.runningAggregates.get(aggregateCount).intValue() / this.runningCount);
                            }
                            break;
                        case Attribute.COUNT:
                            current.add(this.runningCount);
                            break;
                        case Attribute.MIN:
                        case Attribute.MAX:
                            if (projectedType == Attribute.REAL) {
                                current.add(this.runningAggregates.get(aggregateCount).floatValue());
                            } else if (projectedType == Attribute.INT) {
                                current.add(this.runningAggregates.get(aggregateCount).intValue());
                            }
                    }
                    aggregateCount++;
                }
            }
            aggregateCount = 0;
            Tuple outtuple = new Tuple(current);
            result.add(outtuple);

            // If all attributes are aggregated, only need one tuple
            if (attrset.size() == this.runningAggregates.size()) {
                close();
                return result;
            }
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
