    {% macro cent_to_dollar(column_name_parameter) %}
       {{column_name_parameter}}/100
    {% endmacro %}