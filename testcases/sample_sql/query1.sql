SELECT AVG(CUSTOMER.cid),
        MAX(CUSTOMER.gender),
        MAX(CUSTOMER.cid),
        COUNT(CUSTOMER.address),
        MIN(CUSTOMER.firstname),
        CUSTOMER.lastname,
        CUSTOMER.address
FROM CUSTOMER