/**
 * This operator will perform external sorting
 **/

package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;

/**
 * Performs the multi-way merge sort.
 * The number of Batches read/written is equal to 2N*(1 + log(N/B)/log(B-1)).
 */
public class ExternalSort extends Operator {
    private final Operator base;            // base operator
    private final List<OrderByClause> sort_cond;  // list of columns to sort by and whether it's asc or desc.
    private final int buffer_size;          // how many buffer pages for sorting
    private TupleWriter writer;             // the writer to write out our output pages
    private ObjectInputStream reader;          // used for intermediate passes and for returning the result
    private ArrayDeque<String> run_loc;     // temporary files for writing out intermediate sorted runs
    private Comparator<Tuple> comp;         // used for sorting runs
    private final int tuples_per_page;      // number of tuples in a page

    /**
     * Initialises operator for multi-way merge-sort.
     *
     * @param base        The underlying source of tuples
     * @param sort_cond   The list of which attributes to sort by, and whether it's asc or desc
     * @param buffer_size The number of buffer pages allocated for sorting. Must be at least 3.
     * @throws IllegalArgumentException if number of buffer pages is less than 3
     */
    public ExternalSort(Operator base, List<OrderByClause> sort_cond, int buffer_size) throws IllegalArgumentException {
        super(OpType.EXTERNAL_SORT);

        // check if sufficient buffer pages are allocated
        if (buffer_size < 3) throw new IllegalArgumentException("External sort needs at least 3 buffer pages!");
        this.buffer_size = buffer_size;
        this.base = base;
        this.sort_cond = sort_cond;
        this.run_loc = new ArrayDeque<>();
        this.tuples_per_page = Batch.getPageSize() / base.getSchema().getTupleSize();
    }

    /**
     * Performs external sort so that each subsequent call to next() will
     * return sorted Batches
     *
     * @return boolean indicating success or failure of opening the base operator
     */
    @Override
    public boolean open() {
        // try opening the underlying operator
        if (!base.open()) return false;

        // phase 1 - generating sorted runs
        create_sorted_runs();

        // phase 2 - merging sorted runs
        merge_sorted_runs();

        return true;
    }

    /**
     * Extracts batches after sorting has finished.
     *
     * @return the batch of tuples read in by the TupleReader
     */
    @Override
    public Batch next() {
        // use an ObjectInputStream to read in the final sorted run
        if (reader == null) {
            // initialise the stream
            String fileName = run_loc.peek() + ".tbl";
            try {
                reader = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException io) {
                System.out.printf("%s:reading the temporary file error", fileName);
                System.exit(1);
            }
        }

        Batch result = null;
        try {
            result = (Batch) reader.readObject();
        } catch (EOFException e) {
            // No more batch in the file
            return null;
        } catch (ClassNotFoundException c) {
            System.out.printf("%s:Some error in deserialization\n", run_loc.peek() + ".tbl");
            System.exit(1);
        } catch (IOException io) {
            System.out.printf("%s:temporary file reading error\n", run_loc.peek() + ".tbl");
            System.exit(1);
        }

        return result;
    }

    /**
     * Closes the TupleReader and TupleWriter.
     *
     * @return boolean indicating success or failure of de-allocating resource handles
     */
    @Override
    public boolean close() {
        // close the tuple reader
        if (reader != null) {
            try {
                reader.close();
                reader = null; // for gc to collect
            } catch (IOException e) {
                System.out.println("Failed to close the input stream in ExternalSort");
            }
        }

        // close the tuple writer
        if (writer != null) {
            writer.close();
            writer = null; // for garbage collector
        }

        // delete the file storing the completely sorted run
        if (run_loc != null) {
            try {
                File f = new File(run_loc.poll() + ".tbl");
                if (!f.delete()) System.out.println("Could not delete the last sorted run: " + f.getPath());
                run_loc = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    /**
     * Read in B pages each time, sort the records, and produce a B page sorted
     * run (except possibly for the last run). Number of sorted runs = Math.ceil(N / B)
     */
    private void create_sorted_runs() {
        // construct the comparator for tuple-sorting
        comp = (x, y) -> {
            for (OrderByClause clause : sort_cond) {
                // figure out which index to compare x and y on
                int index = base.getSchema().indexOf(clause.getAttr());
                // if they compare differently, return immediately
                int comp = Tuple.compareTuples(x, y, index);
                if (comp != 0) {
                    if (clause.getDirection() == OrderByClause.DESC) comp *= -1;
                    return comp;
                }
            }

            return 0;
        };

        // read in B pages each time
        for (Batch base_page = base.next(); base_page != null && !base_page.isEmpty(); base_page = base.next()) {
            Debug.PPrint(base_page);
            // create the buffer
            ArrayList<Batch> buffer_pages = new ArrayList<>();

            // add the first batch to the buffer
            buffer_pages.add(base_page);

            // fill the buffer to the brim
            while (buffer_pages.size() < buffer_size) {
                if ((base_page = base.next()) == null) break;
                buffer_pages.add(base_page);
            }

            // unpack all the batches
            ArrayList<Tuple> tuples = new ArrayList<>();
            for (Batch b : buffer_pages) {
                for (int i = 0; i < b.size(); ++i) {
                    Debug.PPrint(b.get(i));
                    tuples.add(b.get(i));
                }
            }

            // sort all tuples
            tuples.sort(comp);

            // create a temp file name for this sorted run
            String temp_file_name = "External_Sort_" + run_loc.size();
            System.out.println("External sort line 162: " + temp_file_name);
            run_loc.offer(temp_file_name); // External_Sort_0, External_Sort_1 etc without the .tbl extension

            // use TupleWriter to flush tuples to the temp file
            writer = new TupleWriter(temp_file_name + ".tbl", tuples_per_page);
            writer.open();
            for (Tuple t : tuples) writer.next(t);
            writer.close();
        }

    }

    /**
     * Use B-1 buffer pages for input and one buffer page for output
     * • Perform (B-1)-way merge iteratively until one sorted run is produced
     * • Each iteration requires 1 pass (read + write) over the file
     * • Requires log(N / B)/log(B-1) passes
     */
    private void merge_sorted_runs() {
        // used for indexing merged runs
        int run_index = run_loc.size(); // continue from the previous index

        // while you have at least 2 runs to merge, do a pass
        while (run_loc.size() > 1) {
            // check how many runs you need to merge in this pass
            int runs_to_merge = run_loc.size();

            // while you have at least 2 sorted runs
            while (runs_to_merge > 1) {
                // figure out how many runs you are putting into the buffer
                int num_runs = Math.min(runs_to_merge, buffer_size - 1);

                // use a min-heap for k-way merge sort, and use a TupleReader to extract tuples
                PriorityQueue<TupleReader> min_heap = new PriorityQueue<>((x, y) -> comp.compare(x.peek(), y.peek()));
                for (int i = 0; i < num_runs; ++i) {
                    var reader = new TupleReader(run_loc.poll() + ".tbl", tuples_per_page);
                    reader.open(); // must open for the comparator to use
                    min_heap.offer(reader);
                }

                // create a TupleWriter for your merged run
                String temp_file_name = "External_Sort_" + run_index++;
                System.out.println("External sort line 200: " + temp_file_name);
                run_loc.offer(temp_file_name); // External_Sort_10, External_Sort_11 without the .tbl extension
                writer = new TupleWriter(temp_file_name + ".tbl", tuples_per_page);
                writer.open(); // create the outstream

                // flush tuples into the TupleWriter
                while (!min_heap.isEmpty()) {
                    TupleReader next = min_heap.poll();
                    Tuple next_smallest_tuple = next.next();
                    writer.next(next_smallest_tuple);

                    // re-enqueue the reader if it still has tuples
                    if (next.peek() != null) min_heap.offer(next);
                    else {
                        // delete the file since you are not using it anymore
                        File f = new File(next.getFileName());
                        f.delete();
                    }
                }

                // update the number of runs you have left for this pass
                runs_to_merge -= num_runs;
                // flush this writer to file
                writer.close();
            }
        }
    }

    @Override
    public Object clone() {
        // clone the base
        Operator base_copy = (Operator) base.clone();
        // clone the sort conditions
        List<OrderByClause> sort_cond_copy = new ArrayList<>(sort_cond);
        // clone this operator
        ExternalSort this_clone = new ExternalSort(base_copy, sort_cond_copy, buffer_size);
        // set the schema
        this_clone.setSchema((Schema) base_copy.getSchema().clone());

        return this_clone;
    }
}
