/* use dbt macro to generate table with 10000 rows */
 {{ dbt_utils.generate_series(upper_bound=10000) }}