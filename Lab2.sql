
CREATE TABLE groups (
    id NUMBER NOT NULL,
    group_name VARCHAR2(20) NOT NULL,
    C_VAL NUMBER NOT NULL,
    CONSTRAINT group_id_pkey PRIMARY KEY (id)
);

CREATE TABLE students (
    student_id NUMBER NOT NULL,
    student_name VARCHAR2(20) NOT NULL,
    gr_id NUMBER NOT NULL,
    CONSTRAINT student_id_pkey PRIMARY KEY (student_id),
    CONSTRAINT students_group_id_fkey FOREIGN KEY (gr_id) REFERENCES groups (id)
);


