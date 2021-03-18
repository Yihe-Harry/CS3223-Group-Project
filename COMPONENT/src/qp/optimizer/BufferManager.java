/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    static int numBuffer;
    static int numJoin;

    static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoin) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        // avoid division by zero error
        buffPerJoin = numBuffer / Math.max(numJoin, 1);
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }
}
