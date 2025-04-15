--test1
DROP TABLE test_table CASCADE CONSTRAINTS;

CREATE TABLE test_table (
    id NUMBER,
    name VARCHAR2(50),
    age NUMBER
);

INSERT INTO test_table (id, name, age) VALUES (1, 'Alice', 30);
INSERT INTO test_table (id, name, age) VALUES (2, 'Bob', 25);
INSERT INTO test_table (id, name, age) VALUES (3, 'Charlie', 35);


DECLARE
    v_json CLOB := '{
        "queryType": "SELECT",
        "columns": ["name", "age"],
        "tables": ["test_table"],
        "where": {
            "conditions": [
            {
                "column": "age",
                "operator": ">",
                "value": "25",
                "type": "number"
            }
            ]
        }
        }';
    v_cursor orm_processor.ref_cursor;
    v_id NUMBER;
    v_name VARCHAR2(50);
    v_age NUMBER;
BEGIN
    v_cursor := orm_processor.execute_select_query(v_json);
    
    LOOP
        FETCH v_cursor INTO v_name, v_age;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('Name: ' || v_name || ', Age: ' || v_age);
    END LOOP;
    
    CLOSE v_cursor;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        IF v_cursor%ISOPEN THEN
            CLOSE v_cursor;
        END IF;
END;
/

SELECT name, age FROM test_table WHERE name = 'Alice';