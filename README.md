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
- `AGGREGATE` operator (see [Aggregate.java](src/qp/operators/Aggregate.java))

## Implementation

### 5. Aggregate Operator

The Aggregate operator extends the Projection operator. It, similar to sorting, is a blocking operation. The pipelined approach does not work here. It scans through all the tuples of the underlying operator, accumulates the running aggregates for each aggregated attribute, and stores them internally. When `next()` is called, a Batch of the stored tuples is then released. 

### 6. Bug

When calculating the plan cost of a Join operator, in `PlanCost.getStatistics(Join root)`, the method assumes that the Join condition is an equality condition. It doesn't account for what happens when the condition is actually an inequality, or any other types of conditional. We changed the implementation to consider the number of output tuples when the condition is an inequality, but not when it is a GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL. This is because it is difficult to estimate without the range of values of the attributes. 