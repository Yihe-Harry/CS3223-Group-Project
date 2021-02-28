package qp.utils;

import qp.utils.Attribute;

/**
 * Records the attribute, and whether to sort by asc/desc.
 * Used by ExternalSort.
 */

public class OrderByClause {
    public static final int ASC = 1;
    public static final int DESC = 2;

    private Attribute attr; // the attribute to sort by
    private int direction; // whether asc/desc

    public OrderByClause(Attribute attr, int direction) {
        this.attr = attr;
        this.direction = direction;
    }

    public Attribute getAttr() {
        return attr;
    }

    public int getDirection() {
        return direction;
    }
}
