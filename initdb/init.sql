CREATE TABLE invoices (
    ID SERIAL PRIMARY KEY,
    BillNum VARCHAR(255),
    CreatedTime TIMESTAMP,
    StoreID VARCHAR(255),
    PaymentMode VARCHAR(50),
    TotalValue DOUBLE PRECISION
);

CREATE TABLE store_sales_summary (
    WindowStart TIMESTAMP NOT NULL,
    WindowEnd TIMESTAMP NOT NULL,
    StoreID VARCHAR(255) NOT NULL,
    TotalSales NUMERIC(10, 2) NOT NULL
);
