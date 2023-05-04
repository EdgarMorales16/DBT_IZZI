{% macro dw_control_columns() %}
        current_timestamp() dw_fecha_creacion,
        current_user() dw_creado_por ,
        current_timestamp() as dw_fecha_actualizacion,
        current_user() dw_actualizado_por,
        {{ env_var('DBT_CLOUD_JOB_ID', -1) }}::number as dwjob_id
{% endmacro %}