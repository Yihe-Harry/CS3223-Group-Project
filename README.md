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

### Aggregate Operator

The Aggregate operation is one that occurs just before the Projection operation. It, similar to sorting, is a blocking one. The pipelined approach does not work here. It scans through all the tuples of the underlying operator, accumulates the running aggregates for each aggregated attribute, and stores them internally. When `next()` is called, a Batch of the stored tuples is then released. 