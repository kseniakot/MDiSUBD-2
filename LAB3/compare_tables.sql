CREATE OR REPLACE TYPE dep_rec AS OBJECT (
    table_name VARCHAR2(128),
    depends_on VARCHAR2(128)
);
/

CREATE OR REPLACE TYPE dep_tab AS TABLE OF dep_rec;
/

CREATE OR REPLACE PROCEDURE compare_schemas (
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
) AS
    v_count NUMBER;
    v_cycle_detected BOOLEAN := FALSE;
    v_ddl CLOB;
    v_dependencies dep_tab := dep_tab();

    TYPE table_rec IS RECORD (
        object_name VARCHAR2(128),
        has_cycle BOOLEAN
    );
    TYPE table_tab IS TABLE OF table_rec;
    v_sorted_tables table_tab := table_tab();

--все таблицы которые нужно создать или обновить в проде
    CURSOR object_diff_to_prod IS
        SELECT t.table_name as object_name
        FROM all_tables t
        WHERE t.owner = UPPER(dev_schema_name)
        MINUS
        SELECT t2.table_name
        FROM all_tables t2
        WHERE t2.owner = UPPER(prod_schema_name)
        UNION
        --сравнение структуры таблиц 
        SELECT tc1.table_name
        FROM (
            SELECT table_name,
                   COUNT(column_name) as col_count,
                   LISTAGG(column_name || ':' || data_type, ',') WITHIN GROUP (ORDER BY column_name) as structure
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
        ) tc1;

-- все таблицы, которые есть в проде и нет в леве
    CURSOR object_diff_to_drop IS
        SELECT t.table_name as object_name
        FROM all_tables t
        WHERE t.owner = UPPER(prod_schema_name)
        MINUS
        SELECT t2.table_name
        FROM all_tables t2
        WHERE t2.owner = UPPER(dev_schema_name);

    PROCEDURE topological_sort IS
        TYPE visited_tab IS TABLE OF BOOLEAN INDEX BY VARCHAR2(128);
        v_visited visited_tab;
        v_temp_mark visited_tab;
        TYPE added_tab IS TABLE OF BOOLEAN INDEX BY VARCHAR2(128);
        v_added added_tab;
        v_tables table_tab := table_tab();
        
        PROCEDURE visit(p_table_name IN VARCHAR2) IS
            v_has_cycle BOOLEAN := FALSE;
        BEGIN
            IF v_temp_mark.EXISTS(p_table_name) THEN
                v_cycle_detected := TRUE;
                v_has_cycle := TRUE;
                IF NOT v_added.EXISTS(p_table_name) THEN
                    v_tables.EXTEND;
                    v_tables(v_tables.LAST) := table_rec(p_table_name, v_has_cycle);
                    v_added(p_table_name) := TRUE;
                END IF;
                RETURN;
            END IF;
            IF NOT v_visited.EXISTS(p_table_name) THEN
                v_temp_mark(p_table_name) := TRUE;
                
                FOR i IN 1..v_dependencies.COUNT LOOP
                    IF v_dependencies(i).table_name = p_table_name THEN
                        visit(v_dependencies(i).depends_on);
                    END IF;
                END LOOP;
                v_visited(p_table_name) := TRUE;
                v_temp_mark.DELETE(p_table_name);
                IF NOT v_added.EXISTS(p_table_name) THEN
                    v_tables.EXTEND;
                    v_tables(v_tables.LAST) := table_rec(p_table_name, v_has_cycle);
                    v_added(p_table_name) := TRUE;
                END IF;
            END IF;
        END visit;
    BEGIN
        FOR rec IN object_diff_to_prod LOOP
            IF NOT v_visited.EXISTS(rec.object_name) THEN
                visit(rec.object_name);
            END IF;
        END LOOP;
        v_sorted_tables := v_tables;
        IF v_cycle_detected THEN
            
        DBMS_OUTPUT.PUT_LINE('======================================================');
        DBMS_OUTPUT.PUT_LINE('======================================================');
        DBMS_OUTPUT.PUT_LINE('CYCLE DETECTED');
        END IF;
    END topological_sort;
    
        PROCEDURE sort_tables IS
        TYPE sort_rec IS RECORD (
            object_name VARCHAR2(128),
            has_cycle   BOOLEAN,
            dep_count   NUMBER
        );
        TYPE sort_tab IS TABLE OF sort_rec INDEX BY PLS_INTEGER;
        v_sort sort_tab;
        v_temp sort_rec;
        n PLS_INTEGER := v_sorted_tables.COUNT;
        
        
        FUNCTION has_dependency(a VARCHAR2, b VARCHAR2) RETURN BOOLEAN IS
        BEGIN
            FOR i IN 1..v_dependencies.COUNT LOOP
                IF v_dependencies(i).table_name = a AND v_dependencies(i).depends_on = b THEN
                    RETURN TRUE;
                END IF;
            END LOOP;
            RETURN FALSE;
        END;
        
    BEGIN
        -- заполн массива v_sort
        FOR i IN 1..n LOOP
            v_sort(i).object_name := v_sorted_tables(i).object_name;
            v_sort(i).has_cycle   := v_sorted_tables(i).has_cycle;
            v_sort(i).dep_count   := 0;
            FOR j IN 1..v_dependencies.COUNT LOOP
                IF v_dependencies(j).table_name = v_sorted_tables(i).object_name THEN
                    v_sort(i).dep_count := v_sort(i).dep_count + 1; -- подсчет кол-ва завис для кажд таблицы
                END IF;
            END LOOP;
        END LOOP;
        
        FOR i IN 1..n-1 LOOP
            FOR j IN i+1..n LOOP
                IF v_sort(i).has_cycle = v_sort(j).has_cycle THEN -- чекнем одиаковый ли статус с циклами
                    IF v_sort(i).has_cycle = FALSE THEN --если без цикла то по зависимстям
                        IF v_sort(i).dep_count > v_sort(j).dep_count THEN
                            v_temp := v_sort(i);
                            v_sort(i) := v_sort(j);
                            v_sort(j) := v_temp;
                        ELSIF v_sort(i).dep_count = v_sort(j).dep_count THEN 
                            IF has_dependency(v_sort(i).object_name, v_sort(j).object_name) THEN  -- если табл v_sort(i) завис от v_sort(j) то v_sort(j) будет раньше
                                v_temp := v_sort(i);
                                v_sort(i) := v_sort(j);
                                v_sort(j) := v_temp;
                            END IF;
                        END IF;
                    ELSE 
                        IF v_sort(i).dep_count < v_sort(j).dep_count THEN -- по убыванию в случае циклов
                            v_temp := v_sort(i);
                            v_sort(i) := v_sort(j);
                            v_sort(j) := v_temp;
                        END IF;
                    END IF;
                ELSIF v_sort(i).has_cycle = TRUE AND v_sort(j).has_cycle = FALSE THEN --то табл без цикла раньше чем с
                    v_temp := v_sort(i);
                    v_sort(i) := v_sort(j);
                    v_sort(j) := v_temp;
                END IF;
            END LOOP;
        END LOOP;
        
        v_sorted_tables.DELETE;
        FOR i IN 1..n LOOP
            v_sorted_tables.EXTEND;
            v_sorted_tables(v_sorted_tables.LAST) := table_rec(v_sort(i).object_name, v_sort(i).has_cycle);
        END LOOP;
    END sort_tables;

    
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_users WHERE username = UPPER(dev_schema_name);
    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Dev schema ' || dev_schema_name || ' not exists');
    END IF;
    
    SELECT COUNT(*) INTO v_count FROM all_users WHERE username = UPPER(prod_schema_name);
    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Prod schema ' || prod_schema_name || ' not exists');
    END IF;
    
    -- сбор зависимостей по внеш ключам
    SELECT dep_rec(table_name, referenced_table_name)
    BULK COLLECT INTO v_dependencies
    FROM (
        SELECT DISTINCT
            ac.table_name, -- табл которая содер внеш ключ
            ac2.table_name as referenced_table_name -- имя табл на которую ссылается внешний ключ
        FROM all_constraints ac
        JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name AND ac.owner = acc.owner
        --ac.r_constraint_name — внеш ключ в одной таблице (имя родительского ограничения на кот ссылается текущ огран)
        --ac2.constraint_name — первичный ключ в другой таблице
        JOIN all_constraints ac2 ON ac.r_constraint_name = ac2.constraint_name AND ac2.owner = ac.owner
        WHERE ac.owner = UPPER(dev_schema_name)
          AND ac.constraint_type = 'R'
    ); -- информация, на что ссылается внешн ключ в текущ табл
    
    DBMS_OUTPUT.PUT_LINE('Проверка object_diff_to_prod:');
    FOR rec IN object_diff_to_prod LOOP
        DBMS_OUTPUT.PUT_LINE('Объект: TABLE ' || rec.object_name);
    END LOOP;
    
    topological_sort;
    sort_tables;
    
    -- генерим DDL для таблиц
    FOR i IN 1..v_sorted_tables.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('[DEBUG] Обработка: TABLE ' || v_sorted_tables(i).object_name);
        IF v_sorted_tables(i).has_cycle THEN
            DBMS_OUTPUT.PUT_LINE('-- Replace table: ' || v_sorted_tables(i).object_name || ' (циклическая зависимость)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('-- Replace table: ' || v_sorted_tables(i).object_name);
        END IF;
        DBMS_OUTPUT.PUT_LINE('DROP TABLE "' || UPPER(prod_schema_name) || '"."' || v_sorted_tables(i).object_name || '";');
        
        v_ddl := 'CREATE TABLE "' || UPPER(prod_schema_name) || '"."' || v_sorted_tables(i).object_name || '" (' || chr(10);
        FOR col IN (
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = UPPER(dev_schema_name)
              AND table_name = v_sorted_tables(i).object_name
            ORDER BY column_id
        ) LOOP
            v_ddl := v_ddl || '    "' || col.column_name || '" ' || col.data_type;
            IF col.data_type IN ('VARCHAR2', 'CHAR') THEN
                v_ddl := v_ddl || '(' || col.data_length || ')';
            ELSIF col.data_type = 'NUMBER' AND col.data_precision IS NOT NULL THEN
                v_ddl := v_ddl || '(' || col.data_precision || ',' || NVL(col.data_scale, 0) || ')';
            END IF;
            IF col.nullable = 'N' THEN
                v_ddl := v_ddl || ' NOT NULL';
            END IF;
            v_ddl := v_ddl || ',' || chr(10);
        END LOOP;
        -- генерация ограничений (pk fk)
        FOR cons IN (
            SELECT constraint_name, constraint_type, r_owner, r_constraint_name
            FROM all_constraints
            WHERE owner = UPPER(dev_schema_name)
              AND table_name = v_sorted_tables(i).object_name
              AND constraint_type IN ('P', 'R')
            ORDER BY constraint_type DESC
        ) LOOP
            v_ddl := v_ddl || '    CONSTRAINT "' || cons.constraint_name || '" ';
            IF cons.constraint_type = 'P' THEN
                v_ddl := v_ddl || 'PRIMARY KEY (';
                FOR col IN (
                    SELECT column_name
                    FROM all_cons_columns
                    WHERE owner = UPPER(dev_schema_name)
                      AND constraint_name = cons.constraint_name
                    ORDER BY position
                ) LOOP
                    v_ddl := v_ddl || '"' || col.column_name || '",';
                END LOOP;
                v_ddl := RTRIM(v_ddl, ',') || ')';
            ELSIF cons.constraint_type = 'R' THEN
                v_ddl := v_ddl || 'FOREIGN KEY (';
                FOR col IN (
                    SELECT column_name
                    FROM all_cons_columns
                    WHERE owner = UPPER(dev_schema_name)
                      AND constraint_name = cons.constraint_name
                    ORDER BY position
                ) LOOP
                    v_ddl := v_ddl || '"' || col.column_name || '",';
                END LOOP;
                v_ddl := RTRIM(v_ddl, ',') || ') REFERENCES "' || UPPER(prod_schema_name) || '"."'; 
                DECLARE
                    v_ref_table VARCHAR2(128);
                BEGIN
                    SELECT table_name INTO v_ref_table
                    FROM all_constraints
                    WHERE owner = cons.r_owner
                      AND constraint_name = cons.r_constraint_name
                      AND ROWNUM = 1;
                    v_ddl := v_ddl || v_ref_table || '" (';
                    FOR col IN (
                        SELECT column_name
                        FROM all_cons_columns
                        WHERE owner = cons.r_owner
                          AND constraint_name = cons.r_constraint_name
                        ORDER BY position
                    ) LOOP
                        v_ddl := v_ddl || '"' || col.column_name || '",';
                    END LOOP;
                    v_ddl := RTRIM(v_ddl, ',') || ')'; -- лишнюю запятую скипаем
                EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
                END;
            END IF;
            v_ddl := v_ddl || ',' || chr(10);
        END LOOP;
        
        v_ddl := RTRIM(v_ddl, ',' || chr(10)) || chr(10) || ');';
        DBMS_OUTPUT.PUT_LINE(v_ddl);
        DBMS_OUTPUT.PUT_LINE('/');
    END LOOP;
    
    --дроп тех, что нет в деве
    FOR rec IN object_diff_to_drop LOOP
        DBMS_OUTPUT.PUT_LINE('DROP TABLE "' || UPPER(prod_schema_name) || '"."' || rec.object_name || '";');
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('======================================================');
    DBMS_OUTPUT.PUT_LINE('======================================================');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
        RAISE;
END compare_schemas;
/