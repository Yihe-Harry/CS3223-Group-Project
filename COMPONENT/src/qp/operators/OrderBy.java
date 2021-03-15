package qp.operators;

import qp.utils.Batch;
import qp.utils.OrderByClause;
import qp.utils.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper around ExternalSort for performing order-by. This class
 * helps to segregate the sorting cost for order-by from other operations such as sort-merge join.
 */
public class OrderBy extends Operator {
    private final ExternalSort sorter;             // the operator to sort our tuples
    private final int buffer_size;                 // how many buffer pages for sorting
    private final List<OrderByClause> sort_cond;
    private Operator base;                         // the base operator

    public OrderBy(int type, Operator base, int buffer_size, List<OrderByClause> sort_cond) {
        super(type);
        this.base = base;
        this.buffer_size = buffer_size;
        this.sort_cond = sort_cond;
        sorter = new ExternalSort(base, sort_cond, buffer_size);
    }

    public int getBuffer_size() {
        return buffer_size;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
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

    @Override
    public Object clone() {
        Operator base_copy = (Operator) base.clone();
        List<OrderByClause> cond_copy = new ArrayList<>(sort_cond.size());
        for (OrderByClause c : sort_cond) {
            cond_copy.add((OrderByClause) c.clone());
        }
        OrderBy this_copy = new OrderBy(OpType.ORDER_BY, base_copy, buffer_size, cond_copy);
        this_copy.setSchema((Schema) base.getSchema().clone());
        return this_copy;
    }
}
