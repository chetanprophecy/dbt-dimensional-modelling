WITH salesorderheader AS (

  SELECT * 
  
  FROM {{ ref('salesorderheader')}}

),

stg_salesorderheader AS (

  SELECT 
    DISTINCT (creditcardid) AS creditcardid
  
  FROM salesorderheader AS in0

),

stg_creditcard AS (

  SELECT * 
  
  FROM {{ ref('creditcard')}}

)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['stg_salesorderheader.creditcardid']) }} AS creditcard_key,
  stg_salesorderheader.creditcardid,
  stg_creditcard.cardtype

FROM stg_salesorderheader
LEFT JOIN stg_creditcard
   ON stg_salesorderheader.creditcardid = stg_creditcard.creditcardid

WHERE stg_salesorderheader.creditcardid IS NOT NULL
