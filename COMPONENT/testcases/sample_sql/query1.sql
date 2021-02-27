SELECT AVG(CUSTOMER.cid),
        MAX(CUSTOMER.gender),
        MAX(CUSTOMER.cid),
        COUNT(CUSTOMER.address),
        CUSTOMER.lastname,
        CUSTOMER.address
FROM CUSTOMER