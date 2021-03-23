# CS3223 Database Systems Implementation Project

**_AY2020/2021 Semester 2<br>
School of Computing<br>
National University of Singapore_**

This project focuses on the implementation of a simple _SPJ (Select-Project-Join)_ query engine to illustrate how query processing works in modern database management systems (DBMS), specifically relational databases (RDBMS). More information about the project requirements can be found at the CS3223 Course Website.

This repository presents our approach to this project. We are a team consisting of undergraduate students from the [National University of Singapore](http://www.nus.edu.sg), comprising of

- [Andy Lam](https://github.com/andyylam)
- [Timothy Leong](https://github.com/timothyleong97)
- [Wang Yihe](https://github.com/Yihe-Harry)

## Implementation Outline

Based on the given template, we have implemented the following operators in this SPJ query engine:

- `AGGREGATE` operator (see [Aggregate.java](COMPONENT/src/qp/operators/Aggregate.java))
- `BlockNestedJoin` operator (see [BlockNestedJoin.java](COMPONENT/src/qp/operators/BlockNestedJoin.java))
- `ExternalSort` operator (see [ExternalSort.java](COMPONENT/src/qp/operators/ExternalSort.java)) 
- `Distinct` operator (see [Distinct.java](COMPONENT/src/qp/operators/Distinct.java))
- `OrderBy` operator (see [OrderBy.java](COMPONENT/src/qp/operators/OrderBy.java))
- `SortMergeJoin` operator (see [SortMergeJoin.java](COMPONENT/src/qp/operators/SortMergeJoin.java))

## Implementation

### 1. Aggregate operator

The Aggregate operator extends the Projection operator. It, similar to sorting, is a blocking operation. The pipelined approach does not work here. It scans through all the tuples of the underlying operator, accumulates the running aggregates for each aggregated attribute, and stores them internally. When `next()` is called, a Batch of the stored tuples is then released. 

### 2. BlockNestedJoin operator

This operator utilizes the given Join operator to perform Join with lower cost on the given condition of the join. It 
will read in one block the file from the left of the Join operator first, then iteratively start to read the right side file, each time with one block.
It will then perform Join base on the condition on the two batches, and write the result to the output buffer.
Whenever the output buffer is full, output the result. After one full iteration of the right file, read in the next batch of the left file, and repeat this process until we have read finish the left side file.

### 3. ExternalSort operator

This operator uses the two-phase merge sort approach. It generates sorted runs in the first pass where the size of each run depends on how many buffer pages are available. Then it merges as many runs as possible in each pass until there is only one final run. The sorting is performed during `open()`. This means that external sort cannot support pipelining. When `next()`  is called, this operator then reads in the final sorted run batch by batch.

`ExternalSort` takes in a list of `OrderByClause` objects in its constructor, which just contains an `Attribute` object and a sort direction `ASC/DESC`. Our SPJ parser only supports sorting all attributes in one direction (all ascending or all descending). It then compares tuples in the order of the `OrderByClauses` - for example, if you order by attribute `B` before `A`, then the tuples will be sorted by `B` first then `A`.

### 4. Distinct operator

This operator utilises external sort to group identical tuples together, and then eliminates adjacent copies of tuples. 

### 5. OrderBy operator

This operator utilises ExternalSort to order tuples. When typing queries, make sure to put ASC or DESC on a newline.
You can only select attributes that will appear in the final projection.
> `SELECT *`
>
> `FROM Tbl`
>
> `
> ORDERBY A`

This will order by `A` ascending.

> `SELECT *`
>
> `FROM Tbl
> `
>
> `ORDERBY A
> `
>
> `ASC`

This will order by `A` ascending.

> `SELECT *`
>
> `
> FROM Tbl
> `
>
> `ORDERBY A,B
> `
>
> `DESC`

This will order by `A` descending first, then `B` descending.

### 6. SortMergeJoin operator

This operator utilizes ExternalSort to order the tuples given the conditions of the join. It sorts the left table based on the Attributes of the LHS of the join condition,a nd the right table based on the attributes of the RHS of the join condition. The two sorted tables are then merged together through `Tuple.compare`. 
1. When tuples match, the right table is iterated down, tuples being stored, until the right tuple no longer match with the left tuple. 
2. The left table is then iterated down, joining with the stored right tuples, until they no longer match. Go back to step 1, until either table has been exhausted.

## Others
- `TIME` data format
- Bug

### 1. TIME Data format
> Example table:
> 
> `6`
> 
> `278`
> 
> `flno INTEGER 100 PK 4`
> 
> `from STRING 20 NK 40`
> 
> `to STRING 20 NK 40`
> 
> `distance INTEGER 2500 NK 4`
> 
> `departs TIME 1 NK 95`
> 
> `arrives TIME 1 NK 95`

The TIME data format is represented as a LocalTime object. The datatype is `TIME`, the range value is 
not used (but you have to put some number, e.g. `1`), and the number of bytes used is `95` (derived by counting the size
of LocalTime's data fields). You can use `ORDERBY` on dates. The format of `TIME` is `HH:MM:SS` or `HH:MM`.

### 2. Bug

When calculating the plan cost of a Join operator, in `PlanCost.getStatistics(Join root)`, the method assumes that the Join condition is an equality condition. It doesn't account for what happens when the condition is actually an inequality, or any other types of conditional. We changed the implementation to consider the number of output tuples when the condition is an inequality, but not when it is a GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL. This is because it is difficult to estimate without the range of values of the attributes. 
