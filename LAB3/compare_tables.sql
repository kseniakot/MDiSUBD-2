CREATE OR REPLACE TYPE dep_rec AS OBJECT (
    table_name VARCHAR2(128),
    depends_on VARCHAR2(128)
);
/

CREATE OR REPLACE TYPE dep_tab AS TABLE OF dep_rec; --collection
/

CREATE OR REPLACE PROCEDURE compare_schemas ( -- new tables, updated tables, new procedures
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
) AS
    v_count NUMBER;
    v_cycle_detected BOOLEAN := FALSE;
    v_ddl CLOB; -- variable to store DDL
    v_dependencies dep_tab := dep_tab(); -- initializing variable to store collection of dependencies

    TYPE table_rec IS RECORD (
        object_type VARCHAR2(30),
        object_name VARCHAR2(128),
        has_cycle BOOLEAN
    ); -- something like namedtuple in Python
    TYPE table_tab IS TABLE OF table_rec; -- collection of records
    v_sorted_tables table_tab := table_tab();

    CURSOR object_diff_to_prod IS
        SELECT 'TABLE' as object_type, t.table_name as object_name
        FROM all_tables t
        WHERE t.owner = UPPER(dev_schema_name)
        MINUS
        SELECT 'TABLE', t2.table_name
        FROM all_tables t2
        WHERE t2.owner = UPPER(prod_schema_name)
        UNION
        SELECT 'TABLE', tc1.table_name
        FROM (
            SELECT table_name,
                   COUNT(column_name) as col_count,
                   LISTAGG(column_name || ':' || data_type, ',') WITHIN GROUP (ORDER BY column_name) as structure -- concatenating column names and data types
            FROM all_tab_columns
            WHERE owner = UPPER(dev_schema_name)
            GROUP BY table_name
            MINUS
            SELECT table_name,
                   COUNT(column_name) as col_count,
                   LISTAGG(column_name || ':' || data_type, ',') WITHIN GROUP (ORDER BY column_name) as structure
            FROM all_tab_columns
            WHERE owner = UPPER(prod_schema_name)
            GROUP BY table_name
        ) tc1
        UNION
        SELECT 'PROCEDURE', o1.object_name
        FROM all_objects o1
        WHERE o1.owner = UPPER(dev_schema_name)
        AND o1.object_type = 'PROCEDURE'
        MINUS
        SELECT 'PROCEDURE', o2.object_name
        FROM all_objects o2
        WHERE o2.owner = UPPER(prod_schema_name)
        AND o2.object_type = 'PROCEDURE';


    CURSOR object_diff_to_drop IS
        SELECT 'TABLE' as object_type, t.table_name as object_name
        FROM all_tables t
        WHERE t.owner = UPPER(prod_schema_name)
        MINUS
        SELECT 'TABLE', t2.table_name
        FROM all_tables t2
        WHERE t2.owner = UPPER(dev_schema_name)
        UNION
        SELECT 'PROCEDURE', o1.object_name
        FROM all_objects o1
        WHERE o1.owner = UPPER(prod_schema_name)
        AND o1.object_type = 'PROCEDURE'
        MINUS
        SELECT 'PROCEDURE', o2.object_name
        FROM all_objects o2
        WHERE o2.owner = UPPER(dev_schema_name)
        AND o2.object_type = 'PROCEDURE';




