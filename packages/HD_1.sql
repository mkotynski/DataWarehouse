--------------------------------------------------------
--  DDL for Package Body HD
--------------------------------------------------------

  CREATE OR REPLACE NONEDITIONABLE PACKAGE BODY "SYS"."HD" AS
   
    FUNCTION table_exists(p_table_name IN VARCHAR2, p_owner IN VARCHAR2) RETURN NUMBER
    AS
    v_cnt NUMBER;BEGIN
    SELECT count(table_name) INTO v_cnt
      FROM dba_tables
      WHERE
      upper(owner) = upper(p_owner) AND
      upper(table_name)=upper(p_table_name);
      
      IF v_cnt IS NULL THEN v_cnt := 0; END IF;
      RETURN v_cnt;
    END;

    PROCEDURE from_csv_to_table(v_source IN VARCHAR2, v_table_name IN VARCHAR2, v_path IN VARCHAR2)
    AS
        f utl_file.file_type;
        v_line VARCHAR2 (3000); 
        noofchars NUMBER := 0;
        noofwords NUMBER := 1;
        s         CHAR;
        v_create VARCHAR2(30000);
        v_update VARCHAR2(5000);
        v_insert VARCHAR2(5000);
        v_inserts VARCHAR2(5000);
        v_columns_inserts VARCHAR2(5000);
        v_col   VARCHAR2(2000) :='';
        v_cols_names T_COLUMNS := sys.hd.t_columns();
        v_cols_values T_COLUMNS := sys.hd.t_columns();
        v_cols_types T_COLUMNS := sys.hd.t_columns();
        v_line_index NUMBER :=1;
        v_stmt VARCHAR2(20000):='';
        v_where VARCHAR2(20000):='';
        
        v_help NUMBER :=0;
        v_row_exists NUMBER :=0;
        b VARCHAR2(2000);
        v_iterator NUMBER :=1;
        
        my_cursor SYS_REFCURSOR;
        BEGIN
        f := utl_file.fopen ('DANE', v_path, 'R');
        IF utl_file.is_open(f) THEN
         LOOP
            BEGIN
              utl_file.get_line(f, v_line, 3000);
              IF v_line IS NULL THEN
                EXIT;
              END IF;
              dbms_output.put_line(v_line);
          
                FOR i IN 1..length(v_line)
                LOOP
                    s := substr(v_line, i, 1);
                    IF s != ',' THEN
                         IF i = length(v_line) AND v_line_index = 1 THEN
                         v_col := v_col||s;
                            v_cols_names.extend();
                            v_cols_names(v_cols_names.count) := v_col;
                            v_col:='';
                          ELSIF i = length(v_line) AND v_line_index = 2 THEN
                            v_col := v_col||s;
                            v_cols_types.extend();
                            v_cols_types(v_cols_types.count) := v_col;
                            v_col:='';
                          ELSE
                            v_col := v_col||s;
                        END IF;
                        
                    END IF;
                    -- Count no. of words
                    IF s = ',' THEN
                      noofwords := noofwords + 1;
                      IF v_line_index = 1 THEN
                        v_cols_names.extend();
                        v_cols_names(v_cols_names.count) := v_col;
                      ELSIF v_line_index = 2 THEN
                        v_cols_types.extend();
                        v_cols_types(v_cols_types.count) := v_col;
                      END IF;
                      v_col := '';
                    END IF;
                    
                END LOOP;
                
                dbms_output.put_line('Wczytywanie z pliku');
    
                --COMMIT;
                --dbms_output.put_line('Line id: '||v_line_index);
                IF v_line_index = 2 THEN
                    IF table_exists(concat(v_source,concat('_',v_table_name)), 'STAGE') = 0 THEN
                        v_create := 'CREATE TABLE STAGE.'||v_source||'_'||v_table_name||' ( ';
                        FOR j IN 1..v_cols_names.count LOOP
                            v_create := v_create || v_cols_names(j) || ' ' || v_cols_types(j);
                            IF j != v_cols_names.count THEN
                                v_create := v_create || ',';
                            END IF;
                        END LOOP;
                        v_create:=v_create||', timestamp timestamp, system_id varchar2(255), active number';
                        v_create:=v_create||')';
                        EXECUTE IMMEDIATE v_create;
                        dbms_output.put_line('Utworzono tabele');
                    END IF;
                    --dbms_output.put_line('hihiu');
                ELSIF v_line_index = 1 THEN
                    v_columns_inserts := v_line;
                    dbms_output.put_line('Ustalenie nazwy kolumn');
                ELSIF v_line_index > 2 THEN
                    dbms_output.put_line('Wczytywanie rekordu');
                        v_inserts := v_line;
                
                        v_update := 'UPDATE STAGE.'||v_source||'_'||v_table_name||' ';
                        v_insert := 'INSERT INTO STAGE.'||v_source||'_'||v_table_name||' ( '|| v_columns_inserts || ', timestamp , system_id, active) VALUES (';
                        
                        v_cols_values := f_convert(v_inserts);
                        
                        v_stmt := 'select count(*) from STAGE.'||v_source||'_'||v_table_name||' tmp WHERE id = '||v_cols_values(1)||' and (';-- into v_row_exists;
    
                        my_cursor := sys.hd.return_columns(concat(v_source,concat('_',v_table_name)), 'STAGE', 'tmp');
                           LOOP
                            FETCH my_cursor INTO b;
                            EXIT WHEN my_cursor%NOTFOUND;
                            IF v_iterator <= v_cols_values.count THEN
                           -- dbms_output.put_line('--iter: '||v_iterator);
                                IF v_cols_types(v_iterator) = 'date' THEN
                                    v_where := v_where || ' tmp.' || b || ' = to_date(''' || v_cols_values(v_iterator) || ''',''dd-mm-yyyy'') and';
                                ELSE
                                    IF v_cols_values(v_iterator) LIKE '%null%' THEN
                                         v_where := v_where || ' tmp.' || b || ' is null and';
                                    ELSE
                                        v_where := v_where || ' tmp.' || b || ' = '''||v_cols_values(v_iterator)||''' and';
                                    END IF;
                                END IF;
                                    v_iterator := v_iterator + 1;
                            END IF;
                           END LOOP;
                            v_iterator := 1;
                            v_where := substr(v_where,0,length(v_where)-3);
                            v_stmt := v_stmt || v_where || ')';
                            --dbms_output.put_line(v_stmt);
                            EXECUTE IMMEDIATE v_stmt INTO v_row_exists;
                            v_stmt :='';
                            v_where :='';
                        IF v_row_exists = 0 THEN
                            EXECUTE IMMEDIATE 'select count(*) from STAGE.'||v_source||'_'||v_table_name||' tmp WHERE id = '||v_cols_values(1) INTO v_help;
                            dbms_output.put_line('Wstawianie rekordu');
                            FOR p IN 1..v_cols_values.count
                            LOOP
                            select RTRIM(LTRIM(v_cols_values(p), ' '), ' ') into v_cols_values(p) from dual;
                                IF v_cols_types(p) = 'date' THEN
                                    v_insert := v_insert ||'to_date(''' || v_cols_values(p) || ''',''dd-mm-yyyy'')';
                                ELSE
                                    IF v_cols_values(p) LIKE '%null%' THEN
                                         v_insert := v_insert || 'null';
                                    ELSE
                                        v_insert := v_insert || '''' || v_cols_values(p) || '''';
                                    END IF;
                                END IF;
                                IF p!=v_cols_values.count THEN
                                    v_insert := v_insert || ',';
                                END IF;
                            END LOOP;
                            v_insert := v_insert || ',SYSTIMESTAMP,'''||v_source||''',1)';
                            dbms_output.put_line(v_insert);
                                EXECUTE IMMEDIATE v_insert;
                        END IF;
                            COMMIT;
                            v_insert :='';
                            v_update :='';
                END IF;
                
                END;
                v_line_index := v_line_index + 1;
                v_line := '';
                
              END LOOP;
        END IF;
        utl_file.fclose(f);
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
        dbms_output.put_line('-- Wczytano dane z pliku');
        commit;
    END;
  
  
    FUNCTION generete_dataw_id(p_source_id IN NUMBER, p_system_id IN VARCHAR2, p_target IN VARCHAR2)
    RETURN NUMBER
    IS
        v_count NUMBER:=0;
        v_max NUMBER:=0;
        v_start_dttm DATE:= SYSDATE;
        v_select VARCHAR2(2000);
    BEGIN
        SELECT SYSDATE INTO v_start_dttm FROM dual;
        EXECUTE IMMEDIATE 'select count(*)+1 from '||p_target INTO v_max;
        BEGIN
             v_select := 'select ID from '||p_target||' WHERE system = '''|| p_system_id ||''' AND source_id = '|| p_source_id||' ';
             dbms_output.put_line(v_select);
             EXECUTE IMMEDIATE v_select INTO v_count;
        EXCEPTION
        WHEN no_data_found THEN
            v_count := NULL;
        END;
        IF v_count IS NOT NULL THEN
            RETURN v_count;
        ELSE
            EXECUTE IMMEDIATE 'INSERT INTO '||p_target||' VALUES ('||p_source_id||', '''||p_system_id||''','||v_max||','''||v_start_dttm||''')';
            RETURN v_max;
        END IF;
    
    END;
        
    
    PROCEDURE get_data_from_source(v_source IN VARCHAR2, v_into_schema IN VARCHAR2)
    IS
     CURSOR t_table IS
        SELECT *
        FROM dba_objects
        WHERE object_type = 'TABLE'
        AND owner = v_source;
        TYPE t_tables IS TABLE OF t_table%ROWTYPE;
         v_tables  T_TABLES;
    BEGIN
    dbms_output.put_line(v_into_schema);
        OPEN t_table;
        LOOP
         FETCH t_table bulk collect
         INTO v_tables limit 5;
         EXIT WHEN v_tables.count = 0;
             FOR v_i IN 1..v_tables.count LOOP
               dbms_output.put_line(v_tables(v_i).object_name || ' - ' || v_tables(v_i).owner);
               EXECUTE IMMEDIATE ('CREATE TABLE ' ||v_into_schema||'.'||v_tables(v_i).owner||'_'||v_tables(v_i).object_name||' as (SELECT * FROM '|| v_tables(v_i).owner || '.' || v_tables(v_i).object_name||' where 1=0)');
               EXECUTE IMMEDIATE ('ALTER TABLE ' ||v_into_schema||'.'||v_tables(v_i).owner||'_'||v_tables(v_i).object_name||' ADD uniewazniony varchar2(5)');
               EXECUTE IMMEDIATE ('ALTER TABLE ' ||v_into_schema||'.'||v_tables(v_i).owner||'_'||v_tables(v_i).object_name||' ADD timestamp TIMESTAMP');
               EXECUTE IMMEDIATE ('ALTER TABLE ' ||v_into_schema||'.'||v_tables(v_i).owner||'_'||v_tables(v_i).object_name||' ADD system_id varchar2(255)');
               EXECUTE IMMEDIATE ('ALTER TABLE ' ||v_into_schema||'.'||v_tables(v_i).owner||'_'||v_tables(v_i).object_name||' ADD active number');
               --EXECUTE IMMEDIATE ('DROP TABLE ' ||v_into_schema||'.'||v_tables(v_i).owner||'_'||v_tables(v_i).object_name);
             END LOOP;
        END LOOP;
        CLOSE t_table;
    END;

    
    PROCEDURE sent_data(v_source IN VARCHAR2, v_into_schema IN VARCHAR2)
    IS
     v_insert VARCHAR2(32767);
     CURSOR t_table IS
        SELECT *
        FROM dba_objects
        WHERE object_type = 'TABLE'
        AND owner = v_source;
        TYPE t_tables IS TABLE OF t_table%ROWTYPE;
         v_tables  T_TABLES;
    BEGIN
    dbms_output.put_line(v_into_schema);
        OPEN t_table;
        LOOP
         FETCH t_table bulk collect
         INTO v_tables limit 5;
         EXIT WHEN v_tables.count = 0;
             FOR v_i IN 1..v_tables.count LOOP
               dbms_output.put_line('INSERT INTO '|| v_into_schema||'.'||v_source||'_'||v_tables(v_i).object_name ||' SELECT src.*,''n'',current_timestamp,'''||v_source||''',1 FROM (SELECT '||v_source||'.'||v_tables(v_i).object_name||'.* FROM '|| v_source||'.'||v_tables(v_i).object_name||') src, dual');
               EXECUTE IMMEDIATE 'INSERT INTO '|| v_into_schema||'.'||v_source||'_'||v_tables(v_i).object_name ||' SELECT src.*,''n'',current_timestamp,'''||v_source||''',1 FROM (SELECT '||v_source||'.'||v_tables(v_i).object_name||'.* FROM '|| v_source||'.'||v_tables(v_i).object_name||') src, dual'||
               ' where not exists (select * from '|| v_into_schema||'.'||v_source||'_'||v_tables(v_i).object_name||')';
             END LOOP;
        END LOOP;
        CLOSE t_table;
    END;


    PROCEDURE create_schema(
      pi_username IN NVARCHAR2,
      pi_password IN NVARCHAR2) IS
      
      user_name NVARCHAR2(20)    := pi_username;
      pwd NVARCHAR2(20)     := pi_password;
         li_count       INTEGER  := 0;
         lv_stmt   VARCHAR2 (1000);
    BEGIN
         SELECT count (1)
           INTO li_count
           FROM dba_users
         WHERE username = upper ( user_name );
    
         IF li_count != 0
         THEN
        lv_stmt := 'DROP USER '|| user_name || ' CASCADE';
        EXECUTE IMMEDIATE ( lv_stmt );
         END IF;
            lv_stmt := 'CREATE USER ' || user_name || ' IDENTIFIED BY ' || pwd || ' DEFAULT TABLESPACE SYSTEM';
      dbms_output.put_line(lv_stmt);
    
      EXECUTE IMMEDIATE ( lv_stmt );
                                                    
            -- ****** Object: Roles for user ******
      lv_stmt := 'GRANT RESOURCE, CONNECT TO ' || user_name;
    
      EXECUTE IMMEDIATE ( lv_stmt );
                                                    
            -- ****** Object: System privileges for user ******
      lv_stmt := 'GRANT ALTER SESSION,CREATE ANY TABLE,CREATE CLUSTER,CREATE DATABASE LINK,CREATE MATERIALIZED VIEW,CREATE SYNONYM,CREATE TABLE,CREATE VIEW,CREATE SESSION,UNLIMITED TABLESPACE TO ' || user_name;
    
            EXECUTE IMMEDIATE ( lv_stmt );
            
      COMMIT;
    END;



    PROCEDURE create_temporary(v_target_table VARCHAR2)
    AS
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE TEMP.'||v_target_table||'_tmp AS SELECT * FROM DWH.'||v_target_table||' where 1=0';
        EXECUTE IMMEDIATE 'CREATE TABLE TEMP.'||v_target_table||'_delta AS SELECT * FROM DWH.'||v_target_table||' where 1=0';
        EXECUTE IMMEDIATE 'CREATE TABLE TEMP.'||v_target_table||'_bad AS SELECT * FROM DWH.'||v_target_table||' where 1=0';
        
        FOR RECORD IN(
            SELECT col.table_name,
            col.column_name,
            col.nullable
            FROM sys.all_tab_columns col
            inner join sys.all_tables t ON col.owner = t.owner
            AND col.table_name = t.table_name
            WHERE col.owner = 'TEMP'
            AND upper(col.table_name) LIKE upper('%'||v_target_table||'%')
            AND col.nullable = 'N'
            ORDER BY col.column_id)
        LOOP
            EXECUTE IMMEDIATE 'alter table TEMP.'||RECORD.table_name||' modify '||RECORD.column_name||' null';
        END LOOP;
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_tmp ADD source_id number';
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_delta ADD source_id number';
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_bad ADD source_id number';
            
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_tmp ADD system_id varchar2(255)';
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_delta ADD system_id varchar2(255)';
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_bad ADD system_id varchar2(255)';
            
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_tmp ADD uniewazniony varchar2(2)';
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_delta ADD uniewazniony varchar2(2)';
            EXECUTE IMMEDIATE 'alter table TEMP.'||v_target_table||'_bad ADD uniewazniony varchar2(2)';
            
    END;
    
    
    
    FUNCTION f_convert(p_list IN VARCHAR2)
      RETURN T_COLUMNS
    AS
      l_string       VARCHAR2(32767) := p_list || ',';
      l_comma_index  PLS_INTEGER;
      l_index        PLS_INTEGER := 1;
      l_tab          T_COLUMNS := t_columns();
    BEGIN
      LOOP
       l_comma_index := instr(l_string, ',', l_index);
       EXIT WHEN l_comma_index = 0;
       l_tab.extend;
       l_tab(l_tab.count) := substr(l_string, l_index, l_comma_index - l_index);
       l_index := l_comma_index + 1;
     END LOOP;
     RETURN l_tab;
   END f_convert;
   
       
--------------------------------------------------------------------------
--------------------------------------------------------------------------
------ FUNKCJE I PROCEDURY POTRZEBNE DO PRZEPROWADZENIA PROCESU ETL ------
--------------------------------------------------------------------------
--------------------------------------------------------------------------

    ------------------------------------------------------------------
    -- FUNKCJA ZWRACAJACA NAZWY KOLUMN DANEJ TABELI W POSTACI CIAGU --
    ------------------------------------------------------------------
    FUNCTION return_columns_names(p_table_name IN VARCHAR2, p_owner IN VARCHAR2, p_uniq IN VARCHAR2)
    RETURN VARCHAR2
    AS
    my_cursor SYS_REFCURSOR;
    v_columns VARCHAR(2000):='';
    b VARCHAR2(255);
    BEGIN
        my_cursor := Return_columns(p_table_name, p_owner, p_uniq);
       LOOP
        FETCH my_cursor INTO b;
        EXIT WHEN my_cursor%NOTFOUND;
        v_columns := v_columns || p_uniq || '.' || b || ',';
       END LOOP;
       v_columns := Substr(v_columns,0, Length(v_columns)-1);
        RETURN v_columns;
    END return_columns_names;FUNCTION Return_columns(p_table_name IN VARCHAR2, p_owner IN VARCHAR2, p_uniq IN VARCHAR2)
    RETURN SYS_REFCURSOR
    AS
    ncurs SYS_REFCURSOR;
    BEGIN
            OPEN ncurs FOR 'select col.column_name from sys.all_tab_columns col '||
            'inner join sys.all_tables t on col.owner = t.owner and col.table_name = t.table_name '||
            'where upper(col.owner) = upper('''|| p_owner || ''') and '||
            'upper(col.table_name) = upper('''|| p_table_name ||''') '||
            'order by col.column_id';

        RETURN ncurs;
    END return_columns;
    
    ------------------------------------------------------------------
    -- FUNKCJA SPRAWDZAJACA CZY SA NULLE W WIERSZU -------------------
    ------------------------------------------------------------------
    FUNCTION is_null_in_row(p_table_name IN VARCHAR2, p_owner IN VARCHAR2, p_uniq IN VARCHAR2)
    RETURN VARCHAR2
    AS
    my_cursor SYS_REFCURSOR;
    r_cursor SYS_REFCURSOR;
    stmt VARCHAR(2000);
    v_where VARCHAR(2000);
    v_columns VARCHAR(2000):='';
    b VARCHAR2(255);
    BEGIN
       my_cursor := Return_columns(p_table_name, p_owner, p_uniq);
       LOOP
        FETCH my_cursor INTO b;
        EXIT WHEN my_cursor%NOTFOUND;
        v_where := v_where || ' '|| p_uniq || '.' || b || ' is null or';
       END LOOP;
       stmt := Substr(stmt,0, Length(stmt)-1);
       stmt := 'DELETE FROM ' || p_owner || '.'|| p_table_name || ' ' || p_uniq || ' WHERE ';
       v_where := Substr(v_where,0, Length(v_where)-2);
       stmt := stmt || v_where;
       RETURN stmt;
    END is_null_in_row;
    
    
    ------------------------------------------------------------------
    -- FUNKCJA INSERTUJACA BLEDNE REKORDY(ZAWIERAJACE NULLE) DO BAD --
    ------------------------------------------------------------------
    FUNCTION insert_null_row_to_bad(p_target IN VARCHAR2, p_table_name IN VARCHAR2, p_owner IN VARCHAR2, p_uniq IN VARCHAR2)
    RETURN VARCHAR2
    AS
    my_cursor SYS_REFCURSOR;
    r_cursor SYS_REFCURSOR;
    stmt VARCHAR(2000);
    v_where VARCHAR(2000);
    v_columns VARCHAR(2000):='';
    b VARCHAR2(255);
    BEGIN
       my_cursor := Return_columns(p_table_name, p_owner, p_uniq);
       LOOP
        FETCH my_cursor INTO b;
        EXIT WHEN my_cursor%NOTFOUND;
        v_where := v_where || ' '|| p_uniq || '.' || b || ' is null or';
       END LOOP;
       stmt := Substr(stmt,0, Length(stmt)-1);
       stmt := 'INSERT INTO '||p_target|| ' SELECT * FROM ' || p_owner || '.'|| p_table_name || ' ' || p_uniq || ' WHERE ';
       v_where := Substr(v_where,0, Length(v_where)-2);
       stmt := stmt || v_where;
       RETURN stmt;
    END insert_null_row_to_bad;
END hd;

/
