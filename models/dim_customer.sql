WITH customer AS (

  SELECT * 
  
  FROM {{ ref('customer')}}

),

person AS (

  SELECT * 
  
  FROM {{ ref('person')}}

),

Reformat_3 AS (

  SELECT 
    businessentityid AS businessentityid,
    concat(coalesce(firstname, ''), ' ', coalesce(middlename, ''), ' ', coalesce(lastname, '')) AS fullname
  
  FROM person AS in0

),

store AS (

  SELECT * 
  
  FROM {{ ref('store')}}

),

Reformat_1 AS (

  SELECT 
    businessentityid AS storebusinessentityid,
    storename AS storename
  
  FROM store AS in0

),

Reformat_2 AS (

  SELECT 
    customerid AS customerid,
    personid AS personid,
    storeid AS storeid
  
  FROM customer AS in0

),

Join_1 AS (

  SELECT * 
  
  FROM Reformat_2 AS customer
  LEFT JOIN Reformat_3 AS person
     ON customer.personid = person.businessentityid

),

Join_2 AS (

  SELECT * 
  
  FROM Join_1 AS in0
  LEFT JOIN Reformat_1 AS in1
     ON in0.storeid = in1.storebusinessentityid

),

Reformat_4 AS (

  SELECT 
    {{ dbt_utils.generate_surrogate_key(['customerid']) }} AS customer_key,
    customerid AS customerid,
    personid AS personid,
    storeid AS storeid,
    businessentityid AS businessentityid,
    fullname AS fullname,
    storebusinessentityid AS storebusinessentityid,
    storename AS storename
  
  FROM Join_2 AS in0

)

SELECT *

FROM Reformat_4
