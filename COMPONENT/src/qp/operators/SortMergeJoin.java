package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SortMergeJoin extends Join {
    private ExternalSort left_sorter = null;    // Operator to sort tuples
    private ExternalSort right_sorter = null;   // Operator to sort tuples
    int batchsize;                              // Number of tuples per out batch

    ArrayList<Integer> leftindex;               // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;              // Indices of the join attributes in right table

    int lcurs;                                  // Cursor for left side buffer
    int rcurs;                                  // Cursor for right side buffer
    boolean eosl;                               // Whether end of stream (left table) is reached
    boolean eosr;                               // Whether end of stream (right table) is reached

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        eosl = false;
        eosr = false;

        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        // Create the sort attributes (join conditions) for left
        List<OrderByClause> left_sort_cond = new ArrayList<>();
        for (Condition cond : conditionList) {
            left_sort_cond.add(new OrderByClause(cond.getLhs(), OrderByClause.ASC));
        }

        // Create the sort attributes (join conditions) for right
        List<OrderByClause> right_sort_cond = new ArrayList<>();
        for (Condition cond : conditionList) {
            Attribute a = (Attribute) cond.getRhs();
            right_sort_cond.add(new OrderByClause(a, OrderByClause.ASC));
        }

        // initialise the ExternalSort
        left_sorter = new ExternalSort(left, left_sort_cond, numBuff);
        left_sorter.setSchema(left.getSchema());
        right_sorter = new ExternalSort(right, right_sort_cond, numBuff);
        right_sorter.setSchema(right.getSchema());

        return left_sorter.open() && right_sorter.open();
    }

    public Batch next() {
        // Check if end of either tables has been reached
        if (eosl || eosr) {
            return null;
        }

        // initialise output buffer
        Batch result = new Batch(batchsize);
        Batch leftInbatch = null, rightInbatch = null;

        Tuple currLeftTuple = null;
        ArrayList<Tuple> rightMatchingTuples = new ArrayList<>();   // Right tuples being matched currently
        int rightIndex = 0;                                         // Current index to rightMatchingTuples

        // Keep on checking the incoming pages until result is full
        while (!result.isFull()) {
            if (rightMatchingTuples.size() > 0) {
                // The current left tuple is not done joining with tuples from the right
                if (currLeftTuple != null) {
                    Tuple outtuple = currLeftTuple.joinWith(rightMatchingTuples.get(rightIndex));
                    result.add(outtuple);
                    rightIndex = (rightIndex + 1) % rightMatchingTuples.size();

                    // This current left tuple is done joining
                    if (rightIndex == 0) {
                        currLeftTuple = null;
                    }
                }
                // There are still matching tuples from right table
                // The next left tuple may or may not match with these current tuples from the right
                else {
                    Tuple lefttuple = leftInbatch.get(lcurs);
                    Tuple righttuple = rightMatchingTuples.get(0);
                    int comp = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);

                    // Can match
                    if (comp == 0) {
                        currLeftTuple = lefttuple;
                        Tuple outtuple = currLeftTuple.joinWith(rightMatchingTuples.get(rightIndex));
                        result.add(outtuple);
                        rightIndex = 1;
                    }
                    // If cannot match, then we reset the variables and continue
                    else {
                        currLeftTuple = null;
                        rightIndex = 0;
                        rightMatchingTuples.clear();
                    }
                }
            }
            // No current join is being done. We read in the next tuples
            else {
                // Read in a left page
                if (leftInbatch == null || lcurs == leftInbatch.size()) {
                    leftInbatch = left_sorter.next();
                    lcurs = 0;
                    if (leftInbatch == null) {
                        eosl = true;
                        return result;
                    }
                }
                // Read in a right page
                if (rightInbatch == null || rcurs == rightInbatch.size()) {
                    rightInbatch = right_sorter.next();
                    rcurs = 0;
                    if (rightInbatch == null) {
                        eosr = true;
                        return result;
                    }
                }

                Tuple lefttuple = leftInbatch.get(lcurs);
                Tuple righttuple = rightInbatch.get(rcurs);
                int comp = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);

                // Tuples can be joined
                // Store all matching right tuples into `rightMatchingTuples`
                if (comp == 0) {
                    while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) == 0) {
                        rightMatchingTuples.add(righttuple);
                        rcurs++;
                        if (rcurs == rightInbatch.size()) {
                            rcurs = 0;
                            rightInbatch = right_sorter.next();
                            if (rightInbatch == null) {
                                eosr = true;
                                break;
                            }
                        }
                        righttuple = rightInbatch.get(rcurs);
                        currLeftTuple = lefttuple;
                    }
                } else if (comp < 0) {
                    rcurs++;
                } else {
                    lcurs++;
                }
            }
        }
        return result;
    }

    public boolean close() {
        left_sorter.close();
        right_sorter.close();
        return true;
    }
}