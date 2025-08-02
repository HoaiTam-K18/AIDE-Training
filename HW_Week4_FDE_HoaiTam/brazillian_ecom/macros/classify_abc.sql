{% macro classify_abc(total_bills) %}
    CASE 
        WHEN {{ total_bills }} >= 0 AND {{ total_bills }} < 100 THEN 'D'
        WHEN {{ total_bills }} >= 100 AND {{ total_bills }} < 200 THEN 'C'
        WHEN {{ total_bills }} >= 200 AND {{ total_bills }} < 300 THEN 'B'
        ELSE 'A'
    END
{% endmacro %}