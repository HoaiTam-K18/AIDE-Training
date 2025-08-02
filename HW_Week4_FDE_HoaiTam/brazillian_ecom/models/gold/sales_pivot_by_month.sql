{{ config(materialized="table") }}

WITH source_data AS (

    SELECT 
        monthly,
        category,
        total_bills
    FROM {{ ref("sales_values_by_category") }}

), pivoted_data AS (

    SELECT 
        category,
        {{ dbt_utils.pivot(
            column='monthly',
            values=
            [
                '2016-10', '2016-11', '2016-12',
                '2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06',
                '2017-07', '2017-08', '2017-09', '2017-10', '2017-11', '2017-12',
                '2018-01', '2018-02', '2018-03', '2018-04', '2018-05', '2018-06',
                '2018-07', '2018-08'
            ],
            agg='sum',
            then_value='total_bills'
        ) }}
    FROM source_data
    GROUP BY category

)

SELECT * FROM pivoted_data
