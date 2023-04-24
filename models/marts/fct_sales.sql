WITH salesorderdetail AS (

  SELECT * 
  
  FROM {{ ref('salesorderdetail')}}

),

stg_salesorderdetail AS (

  SELECT 
    salesorderid AS salesorderid,
    salesorderdetailid AS salesorderdetailid,
    productid AS productid,
    orderqty AS orderqty,
    unitprice AS unitprice,
    unitprice * orderqty AS revenue
  
  FROM salesorderdetail AS in0

),

salesorderheader AS (

  SELECT * 
  
  FROM {{ ref('salesorderheader')}}

),

stg_salesorderheader AS (

  SELECT 
    salesorderid AS salesorderid,
    customerid AS customerid,
    creditcardid AS creditcardid,
    shiptoaddressid AS shiptoaddressid,
    status AS order_status,
    CAST(orderdate AS date) AS orderdate
  
  FROM salesorderheader AS in0

),

Join_1 AS (

  SELECT 
    in0.salesorderid AS salesorderid,
    in0.salesorderdetailid AS salesorderdetailid,
    in0.orderqty AS orderqty,
    in0.unitprice AS unitprice,
    in0.revenue AS revenue,
    in1.customerid AS customerid,
    in1.creditcardid AS creditcardid,
    in1.shiptoaddressid AS shiptoaddressid,
    in1.order_status AS order_status,
    in1.orderdate AS orderdate,
    concat(in0.salesorderid, in0.salesorderdetailid) AS custom_key,
    in0.productid AS productid
  
  FROM stg_salesorderdetail AS in0
  INNER JOIN stg_salesorderheader AS in1
     ON in0.salesorderid = in1.salesorderid

)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['custom_key']) }} AS sales_key,
  {{ dbt_utils.generate_surrogate_key(['productid']) }} AS product_key,
  {{ dbt_utils.generate_surrogate_key(['customerid']) }} AS customer_key,
  {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} AS creditcard_key,
  {{ dbt_utils.generate_surrogate_key(['shiptoaddressid']) }} AS ship_address_key,
  {{ dbt_utils.generate_surrogate_key(['order_status']) }} AS order_status_key,
  {{ dbt_utils.generate_surrogate_key(['orderdate']) }} AS order_date_key,
  Join_1.salesorderid,
  Join_1.salesorderdetailid,
  Join_1.unitprice,
  Join_1.orderqty,
  Join_1.revenue

FROM Join_1
