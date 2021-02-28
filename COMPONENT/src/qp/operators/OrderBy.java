package qp.operators;

import qp.utils.Batch;
import qp.utils.OrderByClause;

import java.util.List;

/**
 * A wrapper around ExternalSort for performing order-by. This class
 * helps to segregate the sorting cost for order-by from other operations such as sorted-merge join.
 */
public class OrderBy extends Operator {
    private final Operator base;          // the base operator
    private ExternalSort sorter;          // the operator to sort our tuples
    private final int buffer_size;        // how many buffer pages for sorting
    private final int batch_size;         // number of tuples per batch


    public OrderBy(int type, Operator base, int buffer_size, List<OrderByClause> sort_cond) {
        super(type);
        this.base = base;
        this.buffer_size = buffer_size;
        int tuple_size = base.getSchema().getTupleSize();
        batch_size = Batch.getPageSize() / tuple_size;
        sorter = new ExternalSort(base, sort_cond, buffer_size);
    }

    @Override
    public boolean open() {
        return sorter.open();
    }

    @Override
    public Batch next() {
        return sorter.next();
    }

    @Override
    public boolean close() {
        return sorter.close();
    }
}
