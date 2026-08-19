FROM astrocrpublic.azurecr.io/runtime:3.3-2

RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.12.0 && \
    deactivate