package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;


public class BlockNestedJoin extends Join {
    private static final String WRITING_EXCEPTION_MESSAGE = "Block nested join error: writing file error";
    private static final String READING_EXCEPTION_MESSAGE = "Block nested join error: reading file error";
    private static final String DESERIALIZATION_EXCEPTION_MESSAGE = "Block nested join error: deserialization error";
    //batch is the number of tuples you read in each time
    private int batchSize;

    //index of the join attribute
    private int leftIndex;
    private int rightIndex;

    private String leftFileName;
    private String rightFileName;

    private ObjectInputStream inputStream;

    private static int fileNum = 0;

    //left input buffers, following lecture slides
    private Batch[] leftBatches;
    //right buffer
    private Batch rightBatch;
    //output buffer
    private Batch outBatch;

    private int leftPointer;
    private int rightPointer;

    private boolean isLeftEnd;
    private boolean isRightEnd;

    public BlockNestedJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
        schema = join.getSchema();
        jointype = join.getJoinType();
        numBuff = join.getNumBuff();
    }

    /**
    * Opens the operator
    * Finds the index of the join attributes. materializes the right side into a file, and opens the connections
    * @return true if successfully open
    */
    public boolean open() {
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        Attribute leftAttribute = getCondition().getLhs();
        Attribute rightAttribute = (Attribute) getCondition().getRhs();

        leftIndex = left.getSchema().indexOf(leftAttribute);
        rightIndex = right.getSchema().indexOf(rightAttribute);
        Batch rightPage;

        leftPointer = 0;
        rightPointer = 0;
        isLeftEnd = false;
        isRightEnd = true;

        if (!right.open()) {
            return false;
        } else {
            fileNum++;
            rightFileName = "BlockNested-" + fileNum;
            try {
                ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(rightFileName));
                rightPage = right.next();
                while (rightPage != null) {
                    outputStream.writeObject(rightPage);
                    rightPage = right.next();
                }
                outputStream.close();
            } catch (IOException e) {
                System.out.println(WRITING_EXCEPTION_MESSAGE);
                return false;
            }
            if (!right.close()) {
                return false;
            }
        }
        return left.open();
    }

    @Override
    public Batch next() {
        if (isLeftEnd) {
            close();
            return null;
        }
        outBatch = new Batch(batchSize);
        //While out buffer is not full, keep reading in new left file
        while (!outBatch.isFull()) {
            //Check whether to read a new batch of left file
            if (leftPointer == 0 && isRightEnd) {
                leftBatches = new Batch[numBuff - 2];
                //read in next line from left file
                leftBatches[0] = left.next();
                //Check whether the input is null or not
                if (leftBatches[0] == null) {
                    isLeftEnd = true;
                    return outBatch;
                }
                //Start read in left file
                for (int i = 1; i < leftBatches.length; i++) {
                    leftBatches[i] = left.next();
                    if (leftBatches[i] == null) {
                        break;
                    }
                }
                //Read in the right table when a new left block is being read
                try {
                    inputStream = new ObjectInputStream(new FileInputStream(rightFileName));
                    isRightEnd = false;
                } catch (IOException e) {
                    System.out.println(READING_EXCEPTION_MESSAGE);
                    System.exit(1);
                }
            }
            int leftSize = leftBatches[0].size();
            for (int i = 1; i < leftBatches.length; i++) {
                if (leftBatches[i] == null) {
                    break;
                }
                leftSize += leftBatches[i].size();
            }
            //Read the right table till the end ie, isRightEnd = true
            while (!isRightEnd) {
                try {
                    if (leftPointer == 0 && rightPointer == 0) {
                        rightBatch = (Batch) inputStream.readObject();
                    }
                    for (int i = leftPointer; i < leftSize; i++) {
                        int leftBatchIndex = i / leftBatches[0].size();
                        int leftTupleIndex = i % leftBatches[0].size();
                        Tuple leftTuple = leftBatches[leftBatchIndex].get(leftTupleIndex);

                        for (int j = rightPointer; j < rightBatch.size(); j++) {
                            Tuple rightTuple = rightBatch.get(j);
                            //Check the join condition, if true, add to the output buffer
                            if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
                                Tuple output = leftTuple.joinWith(rightTuple);
                                outBatch.add(output);

                                //Check whether the output buffer is full, if full, return the whole page
                                if (outBatch.isFull()) {
                                    if (i == leftSize - 1 && j == rightBatch.size() - 1) {
                                        leftPointer = 0;
                                        rightPointer = 0;
                                    } else if (i != leftSize - 1 && j == rightBatch.size() - 1) {
                                        leftPointer = i + 1;
                                        rightPointer = 0;
                                    } else {
                                        leftPointer = i;
                                        rightPointer = j + 1;
                                    }
                                    return outBatch;
                                }
                            }
                        }
                        rightPointer = 0;
                    }
                    leftPointer = 0;
                } catch (EOFException e) {
                    try {
                        inputStream.close();
                    } catch (IOException ioException) {
                        System.out.println(READING_EXCEPTION_MESSAGE);
                    }
                    isRightEnd = true;
                } catch (ClassNotFoundException classNotFoundException) {
                    System.out.println(DESERIALIZATION_EXCEPTION_MESSAGE);
                    System.exit(1);
                } catch (IOException ioException) {
                    System.out.println(READING_EXCEPTION_MESSAGE);
                }
            }
        }
        return outBatch;
    }

    @Override
    public boolean close() {
        File f = new File(rightFileName);
        f.delete();
        return true;
    }
}
