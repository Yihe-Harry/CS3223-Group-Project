SELECT AVG(CUSTOMER.cid),
        MAX(CUSTOMER.cid),
        MIN(CUSTOMER.cid),
        CUSTOMER.firstname,
        CUSTOMER.lastname
FROM CUSTOMER