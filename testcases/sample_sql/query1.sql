SELECT AVG(CUSTOMER.cid),
        MAX(CUSTOMER.cid),
        MIN(CUSTOMER.cid),
        COUNT(CUSTOMER.address),
        CUSTOMER.firstname,
        CUSTOMER.lastname
FROM CUSTOMER