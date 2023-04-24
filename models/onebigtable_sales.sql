WITH d_product AS (

  SELECT * 
  
  FROM {{ ref('dim_product')}}

),

d_address AS (

  SELECT * 
  
  FROM {{ ref('dim_address')}}

),

d_customer AS (

  SELECT * 
  
  FROM {{ ref('dim_customer')}}

),

d_order_status AS (

  SELECT * 
  
  FROM {{ ref('dim_order_status')}}

),

d_credit_card AS (

  SELECT * 
  
  FROM {{ ref('dim_credit_card')}}

),

d_date AS (

  SELECT * 
  
  FROM {{ ref('dim_date')}}

),

f_sales AS (

  SELECT * 
  
  FROM {{ ref('fct_sales')}}

),

Join_1 AS (

  SELECT 
    d_product.productid AS productid,
    d_product.product_name AS product_name,
    d_product.productnumber AS productnumber,
    d_product.color AS color,
    d_product.class AS class,
    d_product.product_subcategory_name AS product_subcategory_name,
    d_product.product_category_name AS product_category_name,
    d_customer.customerid AS customerid,
    d_customer.personid AS personid,
    d_customer.storeid AS storeid,
    d_customer.businessentityid AS businessentityid,
    d_customer.fullname AS fullname,
    d_customer.storebusinessentityid AS storebusinessentityid,
    d_customer.storename AS storename,
    d_credit_card.creditcardid AS creditcardid,
    d_credit_card.cardtype AS cardtype,
    d_address.addressid AS addressid,
    d_address.city_name AS city_name,
    d_address.state_name AS state_name,
    d_address.country_name AS country_name,
    d_order_status.order_status AS order_status,
    d_order_status.order_status_name AS order_status_name,
    d_date.date_day AS date_day,
    d_date.prior_date_day AS prior_date_day,
    d_date.next_date_day AS next_date_day,
    d_date.prior_year_date_day AS prior_year_date_day,
    d_date.prior_year_over_year_date_day AS prior_year_over_year_date_day,
    d_date.day_of_week AS day_of_week,
    d_date.day_of_week_name AS day_of_week_name,
    d_date.day_of_month AS day_of_month,
    d_date.day_of_year AS day_of_year,
    f_sales.salesorderid AS salesorderid,
    f_sales.salesorderdetailid AS salesorderdetailid,
    f_sales.unitprice AS unitprice,
    f_sales.orderqty AS orderqty,
    f_sales.revenue AS revenue,
    f_sales.sales_key AS sales_key
  
  FROM f_sales
  LEFT JOIN d_product
     ON f_sales.product_key = d_product.product_key
  LEFT JOIN d_customer
     ON f_sales.customer_key = d_customer.customer_key
  LEFT JOIN d_credit_card
     ON f_sales.creditcard_key = d_credit_card.creditcard_key
  LEFT JOIN d_address
     ON f_sales.ship_address_key = d_address.address_key
  LEFT JOIN d_order_status
     ON f_sales.order_status_key = d_order_status.order_status_key
  LEFT JOIN d_date
     ON f_sales.order_date_key = d_date.date_key

)

SELECT *

FROM Join_1
