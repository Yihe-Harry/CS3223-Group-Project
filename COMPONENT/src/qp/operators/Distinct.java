package qp.operators;

import qp.utils.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utilises external sort to remove duplicate tuples.
 */
public class Distinct extends Operator {
    private final Operator base;            // the base operator
    private ExternalSort sorter = null;     // the operator to sort our tuples
    private final int buffer_size;          // how many buffer pages for sorting
    private final int batch_size;           // number of tuples per batch

    private Batch inbatch;                  // buffer to hold un-flushed tuples
    private int start;                      // cursor position in the input buffer
    boolean eos;                            // checks if the underlying stream has been exhausted

    public Distinct(int type, Operator base, int buffer_size) {
        super(type);
        this.base = base;
        this.buffer_size = buffer_size;
        int tuple_size = base.getSchema().getTupleSize();
        batch_size = Batch.getPageSize() / tuple_size;
    }

    public Operator getBase() {
        return base;
    }

    public int getBuffer_size() {
        return buffer_size;
    }

    @Override
    public boolean open() {
        // create a list of OrderByClauses. The ASC/DESC property is unimportant because you just want
        // tuples with the same values to be adjacent to each other.
        List<OrderByClause> sort_cond = new ArrayList<>();
        Schema underlying_schema = base.getSchema();
        for (Attribute attr : underlying_schema.getAttList()) {
            sort_cond.add(new OrderByClause(attr, OrderByClause.ASC));
        }

        // initialise the ExternalSort
        sorter = new ExternalSort(base, sort_cond, buffer_size);
        eos = false;
        start = 0;


        // try opening the sorter
        return sorter.open();
    }

    /**
     * Code here is similar to Select in that I need to read in Batches of Tuples until I have
     * a full Batch to return
     */
    @Override
    public Batch next() {
        // check if end-of-stream has been reached
        if (eos) {
            close();
            return null;
        }

        // initialise output buffer
        Batch result = new Batch(batch_size);

        // keep on checking the incoming pages until result is full
        int i = 0;
        while (!result.isFull()) {
            if (start == 0) {
                inbatch = base.next();
                // if inbatch is null, the underlying stream has been exhausted
                if (inbatch == null) {
                    eos = true;
                    return null; // here I deviate from Select because no point returning empty page
                }
            }

            // read in pages until either result is full or this page is fully checked
            Tuple previous = null;
            for (i = start; i < inbatch.size() && !result.isFull(); ++i) {
                Tuple present = inbatch.get(i);
                if (!isSame(previous, present)) {
                    previous = present;
                    result.add(present);
                }
            }

            // adjust cursor for the next call to next()
            if (i == inbatch.size()) {
                start = 0;
            } else {
                start = i;
            }
        }

        return result;
    }

    @Override
    public boolean close() {
        // close the base and the ExternalSort
        base.close();
        sorter.close();
        return true;
    }

    /**
     * Checks if two tuples agree at every index.
     * @param left first tuple
     * @param right second tuple
     * @return true if the two tuples agree at every index, false otherwise.
     */
    private boolean isSame(Tuple left, Tuple right) {
        // previous is initially null so this guards against that case
        if (Objects.isNull(left) != Objects.isNull(right)) return false;

        int num_columns = left.data().size();
        for (int i = 0; i < num_columns; ++i) {
            if (Tuple.compareTuples(left, right, i) != 0) return false;
        }

        return true;
    }
}
