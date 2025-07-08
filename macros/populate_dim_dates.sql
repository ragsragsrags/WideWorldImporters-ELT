{%- macro populate_dim_dates(database, cutofftime_table) -%}
    {% set sql_statement %}
    WITH cutofftimes AS (
        
        {{ get_cutofftimes(database, cutofftime_table) }}

    )

    SELECT TOP 1 
        DATEDIFF (
            DAY, 
            last_cutofftime, 
            cutofftime
        ) + 1 AS days
    FROM
        cutofftimes  
    {% endset %}
    {% set days = dbt_utils.get_single_value(sql_statement) | int %}
    
    {% set sql_statement_cutofftime %}
    WITH cutofftimes AS (
        
        {{ get_cutofftimes(database, cutofftime_table) }}

    )

    SELECT 
        last_cutofftime
    FROM
        cutofftimes 
    {% endset %}
    {% set last_cutofftime = dbt_utils.get_single_value(sql_statement_cutofftime) %}

    WITH dates AS (

        {% for n in range(days) %}
        SELECT 
            DATEADD (
                DAY,
                {{ n }}, 
                CAST('{{ last_cutofftime }}' AS Timestamp_NTZ)
            ) AS date
            {% if not loop.last %}
            UNION ALL
            {% endif %}
        {% endfor %}  
    )

    SELECT
        date,
        DAY(date) AS day_number,
        DAY(date) AS day,
        TO_CHAR(date, 'MMMM') AS month,
        MONTHNAME(date) AS short_month,
        MONTH(date) AS calendar_month_number,
        CONCAT(
            'CY', 
            TO_CHAR(YEAR(date)), 
            '-',
            MONTHNAME(date)
        ) AS calendar_month_label,
        YEAR(date) AS calendar_year,
        CONCAT(
            'CY',
            TO_CHAR(YEAR(date))
        ) AS calendar_year_label,
        CASE 
            WHEN MONTH(date) IN (11, 12) THEN MONTH(date) - 10
            ELSE MONTH(date) + 2
        END AS fiscal_month_number,
        CONCAT(
            'FY',
            CAST(
                CASE 
                    WHEN MONTH(date) IN (11, 12) THEN YEAR(date) + 1
                    ELSE YEAR(date)
                END AS VARCHAR
            ),
            '-',
            MONTHNAME(date)
        ) AS fiscal_month_label,
        CASE 
            WHEN MONTH(date) IN (11, 12) THEN YEAR(date) + 1
            ELSE YEAR(date)
        END AS fiscal_year,
        CONCAT(
            'FY',
            CAST(
                CASE 
                    WHEN MONTH(date) IN (11, 12) THEN YEAR(date) + 1
                    ELSE YEAR(date)
                END AS VARCHAR
            )
        ) AS fiscal_year_label,
        DATE_PART(WEEKISO, date) AS iso_week_number
    FROM
        dates
{%- endmacro -%}