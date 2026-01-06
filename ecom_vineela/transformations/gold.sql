
create materialized view vineela_gold.customers_active as 
select * except(`__START_AT`,`__END_AT`) from lakehouse.vineela_silver.customers_scd2 where `__END_AT` is null;



create view vineela_gold.customers_inactive as 
select * except(`__START_AT`,`__END_AT`)  from lakehouse.vineela_silver.customers_scd2 where `__END_AT` is not null;


create materialized view vineela_gold.top3_allgold as 
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  p.product_id,
  p.product_name,
  p.product_category,
  p.product_price,
  ROUND(SUM(s.total_amount)) AS total_amount
FROM
  lakehouse.vineela_silver.sales_cleaned s
JOIN
  lakehouse.vineela_gold.customers_active c
    ON s.customer_id = c.customer_id
JOIN
  lakehouse.vineela_silver.product_scd1 p
    ON s.product_id = p.product_id
GROUP BY
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  p.product_id,
  p.product_name,
  p.product_category,
  p.product_price
ORDER BY
  total_amount DESC
LIMIT 3;


create materialized view vineela_gold.top3_customer as 
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  ROUND(SUM(s.total_amount)) AS total_amount
FROM
  lakehouse.vineela_silver.sales_cleaned s
JOIN
  lakehouse.vineela_gold.customers_active c
ON
  s.customer_id = c.customer_id
GROUP BY
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state
ORDER BY
  total_amount DESC
LIMIT 3
