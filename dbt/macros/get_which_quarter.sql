{% macro get_which_quarter(month) %}
    case {{dbt.safe_cast("month", api.Column.translate_type("integer"))}}
        when 1 then "Q1"
        when 2 then "Q1"
        when 3 then "Q3"
        when 4 then "Q2"
        when 5 then "Q2"
        when 6 then "Q2"
        when 7 then "Q3"
        when 8 then "Q3"
        when 9 then "Q3"
        when 10 then "Q4"
        when 11 then "Q4"
        when 12 then "Q4"
        else "Empty"
    end
{% endmacro %}