#! /bin/sh

java RandomDB BILL 10
java RandomDB CUSTOMER 10
java RandomDB CART 10
java RandomDB CARTDETAILS 10

java ConvertTxtToTbl BILL
java ConvertTxtToTbl CUSTOMER
java ConvertTxtToTbl CART
java ConvertTxtToTbl CARTDETAILS
