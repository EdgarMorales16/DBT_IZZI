{% macro fnc_var_orig_id(var_orig_id) -%}

{%- set orig_id_query -%}
select distinct id from raw_crm.ttc_cat_org_id
order by 1
{%- endset -%}

{%- set results = run_query(orig_id_query) -%}

{%- if execute -%}
{# Return the first column #}
{%- set results_list = results.columns[0].values() -%}
{%- else -%}
{%- set results_list = [] -%}
{%- endif -%}

{%- if var('var_orig_id') is none -%}
null
{%- elif var('var_orig_id') | string in results_list | string -%}
{{var_orig_id}}
{%- else -%}
ERROR
{%- endif -%}

{% endmacro %}