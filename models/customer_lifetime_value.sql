{{
  config(
    materialized = "view",
    meta = {
      "continual": {
        "type": "Model",
        "description": "Predict customer active total spend lifetime",
        "index": "customer_id",
        "target": "customer_lifetime_value",
        "columns": [
          {"name": "customer_id", "entity": "customer"}
        ]
      }
    }
  )  
}}

SELECT
    customer_id,
    customer_lifetime_value
FROM
    {{ ref("customers") }}