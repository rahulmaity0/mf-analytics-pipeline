{#- Small generic tests, written here to avoid pulling in a package for three checks. -#}

{% test non_negative(model, column_name) %}
select * from {{ model }} where {{ column_name }} < 0
{% endtest %}

{% test non_positive(model, column_name) %}
select * from {{ model }} where {{ column_name }} > 0
{% endtest %}

{% test unique_combination_of_columns(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
