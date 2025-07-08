{%- macro get_table_changes(database, cutofftimeTable, mainModel, archiveModel, validFromColumn, validToColumn, keyColumn) -%}
    WITH cutofftimes AS (

        {{ get_cutofftimes(database, cutofftimeTable) }}

    ),

    main AS (

        SELECT
            *
        FROM
            {{ ref(mainModel) }} AS main
        WHERE
            EXISTS (
                SELECT 
                    1
                FROM
                    cutofftimes
                WHERE
                    (
                        main.{{ validFromColumn }} > cutofftimes.last_cutofftime OR
                        (
                            cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                            main.{{ validFromColumn }} >= cutofftimes.last_cutofftime  
                        ) 
                    ) AND
                    main.{{ validFromColumn }} <= cutofftimes.cutofftime       
            )

    ),

    {%- if archiveModel != "" -%}
    archive AS (

        SELECT
            *
        FROM
            {{ ref(archiveModel) }} AS archive
        WHERE
            EXISTS (
                SELECT 
                    1
                FROM
                    cutofftimes
                WHERE
                    (
                        archive.{{ validFromColumn }} > cutofftimes.last_cutofftime OR
                        (
                            cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                            archive.{{ validFromColumn }} >= cutofftimes.last_cutofftime  
                        ) 
                    ) AND
                    archive.{{ validFromColumn }} <= cutofftimes.cutofftime       
            ) 

    ),

    archive_max_valid_from AS (
        SELECT
            archive.{{ keyColumn }},
            MAX(archive.{{ validFromColumn }}) AS {{ validFromColumn }},
            MAX(archive.{{ validToColumn }}) AS {{ validToColumn }}
        FROM
            archive
        GROUP BY
            archive.{{ keyColumn }} 
    ),
    {%- endif -%}

    final AS (
        
        SELECT
            main.*
        FROM
            main    

        {%- if archiveModel != "" %}
        
        UNION ALL

        SELECT
            archive.*
        FROM
            archive
        WHERE
            archive.{{ keyColumn }} NOT IN (
                SELECT
                    {{ keyColumn }}
                FROM 
                    main
            ) AND
            EXISTS (
                SELECT
                    1
                FROM
                    archive_max_valid_from
                WHERE
                    archive_max_valid_from.{{ keyColumn }} = archive.{{ keyColumn }} AND
                    archive_max_valid_from.{{ validFromColumn }} = archive.{{ validFromColumn }} AND
                    archive_max_valid_from.{{ validToColumn }} = archive.{{ validToColumn }}
            )
        {%- endif -%}
            
    )

    SELECT 
        *
    FROM
        final

{%- endmacro -%}