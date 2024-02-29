-- create_invoices_table.sql

CREATE TABLE IF NOT EXISTS invoices (
    ID SERIAL PRIMARY KEY,
    BillNum VARCHAR(255),
    CreatedTime TIMESTAMP,
    StoreID VARCHAR(255),
    PaymentMode VARCHAR(50),
    TotalValue DOUBLE PRECISION
);
