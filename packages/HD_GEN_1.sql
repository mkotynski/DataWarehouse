--------------------------------------------------------
--  DDL for Package Body HD_GEN
--------------------------------------------------------

  CREATE OR REPLACE NONEDITIONABLE PACKAGE BODY "SYS"."HD_GEN" IS

    PROCEDURE to_tmp(p_source_table IN VARCHAR2, p_into_table IN VARCHAR2, p_columns IN VARCHAR2, p_id IN VARCHAR2, p_system IN VARCHAR2, p_key_table IN VARCHAR2)
    AS
    BEGIN
--        execute immediate
         execute immediate('INSERT INTO temp.'||p_into_table||'_tmp'||
        ' SELECT '||p_id||', '||p_columns||', SYSDATE, to_date(''9999-12-31'',''yyyy-mm-dd''), '||p_id||', '''||p_system||''', uniewazniony'||
        ' FROM stage.'||p_source_table||
        ' WHERE to_char(to_date(substr(to_char(timestamp),0,14),''DD/MM/YY HH24:MI'')) IN ('||
        ' SELECT to_char(min(to_date(package_stamp,''YY/MM/DD HH24:MI''))) AS stamp FROM (SELECT to_date(substr(to_char(timestamp),0,14),''DD/MM/YY HH24:MI'') AS package_stamp'||
        ' FROM stage.'||p_source_table||' WHERE active = 1))');
  dbms_output.put_line('INSERT INTO temp.'||p_into_table||'_tmp'||
        ' SELECT sys.hd.Generete_dataw_id('||p_id||','''||p_system||''',''dwh.'||p_key_table||'''), imie, nazwisko, data_zatrudnienia, data_zwolnienia, pesel, SYSDATE, to_date(''9999-12-31'',''yyyy-mm-dd''), '||p_id||', '''||p_system||''', uniewazniony'||
        ' FROM stage.'||p_source_table||
        ' WHERE to_char(to_date(substr(to_char(timestamp),0,14),''DD/MM/YY HH24:MI'')) IN ('||
        ' SELECT to_char(min(to_date(package_stamp,''DD/MM/YY HH24:MI''))) AS stamp FROM (SELECT to_date(substr(to_char(timestamp),0,14),''DD/MM/YY HH24:MI'') AS package_stamp'||
        ' FROM stage.'||p_source_table||' WHERE active = 1))');
--        dbms_output.put_line(' SELECT min(package_stamp) AS stamp FROM (SELECT to_date(substr(to_char(timestamp),0,14),''DD/MM/YY HH24:MI'') AS package_stamp'||
--        ' FROM stage.'||p_source_table||' WHERE active = 1))');
--            
        execute immediate('UPDATE stage.'||p_source_table||' SET active = 0 WHERE To_char(To_date(Substr(To_char(timestamp),0,14),''DD/MM/YY HH24:MI'')) IN ('||
        ' SELECT to_char(min(to_date(package_stamp,''YY/MM/DD HH24:MI''))) AS stamp FROM (SELECT To_date(Substr(To_char(timestamp),0,14),''DD/MM/YY HH24:MI'') AS package_stamp'||
        ' FROM stage.'||p_source_table||' WHERE active = 1))');
    END;

    PROCEDURE to_delta(p_table IN VARCHAR2, p_columns IN VARCHAR2)
    AS
    v_stmt VARCHAR2(20000):='';
    v_join VARCHAR2(20000):='';
    v_target_name VARCHAR2(255):='przerwa_w_pracy';--przerwa_w_pracy , zamowienie
    v_columns sys.hd.t_columns := sys.hd.t_columns();
    cur sys_refcursor;
    rec VARCHAR2(255);
    v_iter NUMBER := 1;
    BEGIN
        v_target_name:=p_table;
        v_columns := sys.hd.f_convert(p_columns);
        v_stmt := 'insert into temp.'||v_target_name||'_delta '||
        'select '||p_columns||',sysdate,to_date(''31-12-9999'',''DD-MM-YYYY''),source_id, system_id, uniewazniony from '||
        '(select ';

        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'tmp.'||v_columns(p)||',';
        END LOOP;
            v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
            v_stmt:= v_stmt || ',tmp.source_id,tmp.system_id,uniewazniony from temp.'||v_target_name||'_tmp tmp ';

        FOR rec IN (SELECT ft.nazwa_tabela AS rodzic,wy.nazwa_tabela AS dziecko,wy.nazwa_kolumna_id FROM dwh_meta.rodzic_dziecko wf
                        join dwh_meta.tabela ft ON ft.id = wf.id_rodzic
                        join dwh_meta.tabela wy ON wy.id = wf.id_dziecko
                        WHERE upper(ft.nazwa_tabela) = upper(v_target_name))
        LOOP
           v_join := v_join || 'join dwh.'|| rec.dziecko||' t'||v_iter||' on t'||v_iter||'.source_id = tmp.'||rec.nazwa_kolumna_id||' and t'||v_iter||'.system = tmp.system_id ';
        END LOOP;
        v_stmt:=v_stmt||v_join;

        v_stmt:=v_stmt||' minus ';

        v_stmt:=v_stmt||'select src.source_id as "'||upper(v_columns(1))||'",';
        FOR p IN 2..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'tgt.'||v_columns(p)||',';
        END LOOP;
        v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
        v_stmt:= v_stmt || ',tmp.source_id,tmp.system_id,''n'' from dwh.'||v_target_name||' tgt ';

        v_stmt:= v_stmt || ' join dwh.'||v_target_name||'_key src on src.id = tgt.'||v_columns(1);
        v_stmt:= v_stmt || ' join temp.'||v_target_name||'_tmp tmp on src.id = tmp.'||v_columns(1)||' and src.system = tmp.system_id';

        v_stmt:= v_stmt || ' where src.source_id = tmp.'||v_columns(1)||' ';
        FOR p IN 2..v_columns.count
        LOOP
            v_stmt:= v_stmt ||' and tgt.'||v_columns(p)||' = tmp.'||v_columns(p);
        END LOOP;

        v_stmt:= v_stmt || ' and not exists( select ';
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'delta.'||v_columns(p)||',';
        END LOOP;
        v_stmt:= v_stmt || 'source_id,system_id from temp.'||v_target_name||'_delta delta ';
        v_stmt:= v_stmt || ' where tmp.'||v_columns(1)||' = delta.'||v_columns(1)||'))';
         v_stmt:= v_stmt || ' where ' || v_columns(1) ||' is not null';
        FOR p IN 2..v_columns.count
        LOOP
            v_stmt:= v_stmt ||' and ' || v_columns(p)||' is not null ';
        END LOOP;
        dbms_output.put_line(v_stmt);
        EXECUTE IMMEDIATE v_stmt;
        v_stmt:='';
        v_join := '';
        v_iter := 1;
        v_stmt := 'insert into temp.'||v_target_name||'_delta '||
        'select '||p_columns||',sysdate,to_date(''31-12-9999'',''DD-MM-YYYY''),source_id, system_id, uniewazniony from '||
        '(select ';
        
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'bad.'||v_columns(p)||',';
        END LOOP;
            v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
            v_stmt:= v_stmt || ',bad.source_id,bad.system_id,uniewazniony from temp.'||v_target_name||'_bad bad ';
            
               
        FOR rec IN (SELECT ft.nazwa_tabela AS rodzic,wy.nazwa_tabela AS dziecko,wy.nazwa_kolumna_id FROM dwh_meta.rodzic_dziecko wf
                        join dwh_meta.tabela ft ON ft.id = wf.id_rodzic
                        join dwh_meta.tabela wy ON wy.id = wf.id_dziecko
                        WHERE upper(ft.nazwa_tabela) = upper(v_target_name))
        LOOP
           v_join := v_join || 'join dwh.'|| rec.dziecko||' t'||v_iter||'  on t'||v_iter||' .source_id = bad.'||rec.nazwa_kolumna_id||' and t'||v_iter||'.system = bad.system_id ';
        END LOOP;
        v_stmt:=v_stmt||v_join;
        
        v_stmt:=v_stmt||'where not exists ( ';
        
        v_stmt:=v_stmt||'select src.source_id as "'||upper(v_columns(1))||'",';
        FOR p IN 2..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'tgt.'||v_columns(p)||',';
        END LOOP;
        v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
        v_stmt:= v_stmt || ',bad.source_id,bad.system_id,''n'' from dwh.'||v_target_name||' tgt ';
        
        v_stmt:= v_stmt || ' join dwh.'||v_target_name||'_key src on src.id = tgt.'||v_columns(1);
        v_stmt:= v_stmt || ' join temp.'||v_target_name||'_tmp tmp on src.id = bad.'||v_columns(1)||' and src.system = bad.system_id';
        
        v_stmt:= v_stmt || ' where src.source_id = tmp.'||v_columns(1)||' ';
        FOR p IN 2..v_columns.count
        LOOP
            v_stmt:= v_stmt ||' and tgt.'||v_columns(p)||' = bad.'||v_columns(p);
        END LOOP;
        
        v_stmt:= v_stmt || ') and not exists( select ';
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'delta.'||v_columns(p)||',';
        END LOOP;
        v_stmt:= v_stmt || 'source_id,system_id from temp.'||v_target_name||'_delta delta ';
        v_stmt:= v_stmt || ' where bad.'||v_columns(1)||' = delta.'||v_columns(1)||'))';
        
        v_stmt:= v_stmt || ' where ' || v_columns(1) ||' is not null';
        FOR p IN 2..v_columns.count
        LOOP
            v_stmt:= v_stmt ||' and ' || v_columns(p)||' is not null ';
        END LOOP;

        dbms_output.put_line(v_stmt);
        EXECUTE IMMEDIATE v_stmt;
    END;
    
    PROCEDURE to_bad(p_table IN VARCHAR2, p_columns IN VARCHAR2)
    AS
    v_stmt VARCHAR2(20000):='';
    v_join VARCHAR2(20000):='';
    v_target_name VARCHAR2(255):='przerwa_w_pracy';--przerwa_w_pracy , zamowienie
    v_columns_text VARCHAR2(255) := 'ID_PRACOWNIKA,ID_PRZERW_W_PRACY,ID_RODZAJ_PRZERWY,ID_UZASADNIENIA,CZAS_TRWANIA'; ---id_zamowienie,id_klient,id_pracownik,data; 
    v_columns sys.hd.t_columns := sys.hd.t_columns();
    cur SYS_REFCURSOR;
    rec VARCHAR2(255);
    v_iter NUMBER := 1;
    BEGIN
        v_target_name := p_table;
        v_columns := sys.hd.f_convert(p_columns);
        v_stmt := 'insert into temp.'||v_target_name||'_bad '||
        'select '||p_columns||',sysdate,to_date(''31-12-9999'',''DD-MM-YYYY''),source_id, system_id, uniewazniony from '||
        '(select ';
        
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'bad.'||v_columns(p)||',';
        END LOOP;
            v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
            v_stmt:= v_stmt || ',bad.source_id,bad.system_id,uniewazniony from temp.'||v_target_name||'_bad bad ';
  
        v_stmt:=v_stmt||' where exists ( select ';
        
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'tgt.'||v_columns(p)||',';
        END LOOP;
        v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
        v_stmt:= v_stmt || ',tmp2.source_id, tmp2.system_id,uniewazniony from dwh.'||v_target_name||' tgt, temp.'||v_target_name||'_tmp tmp2 ';
        
        v_stmt:= v_stmt || ' join dwh.'||v_target_name||'_key zk on zk.source_id = tmp2.'||v_columns(1)||' and zk.system = tmp2.system_id';
        v_stmt:= v_stmt || ' join dwh.'||v_target_name||'_key zk2 on zk2.id = '||v_columns(1);

        v_stmt:= v_stmt || ') and not exists( select ';
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'bad.'||v_columns(p)||',';
        END LOOP;
        v_stmt:= v_stmt || 'source_id,system_id from temp.'||v_target_name||'_bad bad ';
        v_stmt:= v_stmt || ' where bad.'||v_columns(1)||' = bad.'||v_columns(1)||'))';

        dbms_output.put_line(v_stmt);
        EXECUTE IMMEDIATE v_stmt;
        
        v_stmt := '';
        v_join:='';
        v_stmt := 'insert into temp.'||v_target_name||'_bad '||
        'select '||p_columns||',sysdate,to_date(''31-12-9999'',''DD-MM-YYYY''),source_id, system_id, uniewazniony from '||
        '(select ';
        
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'tmp3.'||v_columns(p)||',';
        END LOOP;
            v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
            v_stmt:= v_stmt || ',tmp3.source_id,tmp3.system_id,uniewazniony from temp.'||v_target_name||'_tmp tmp3 ';
                      
        v_stmt:= v_stmt ||' where not exists (select ';
        
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'tmp.'||v_columns(p)||',';
        END LOOP;
            v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
            v_stmt:= v_stmt || ',tmp.source_id,tmp.system_id,uniewazniony from temp.'||v_target_name||'_tmp tmp ';
                            
        FOR rec IN (SELECT ft.nazwa_tabela AS rodzic,wy.nazwa_tabela AS dziecko,wy.nazwa_kolumna_id FROM dwh_meta.rodzic_dziecko wf
                        join dwh_meta.tabela ft ON ft.id = wf.id_rodzic
                        join dwh_meta.tabela wy ON wy.id = wf.id_dziecko
                        WHERE upper(ft.nazwa_tabela) = upper(v_target_name))
        LOOP
           v_join := v_join || 'join dwh.'|| rec.dziecko||' t'||v_iter||' on t1.source_id = tmp.'||rec.nazwa_kolumna_id||' and t'||v_iter||'.system = tmp.system_id ';
           v_iter:=v_iter+1;
        END LOOP;
        v_stmt:=v_stmt||v_join;
        
        v_stmt:=v_stmt||'  where tmp3.'||v_columns(1)||' = tmp.'||v_columns(1)||' )';
        
        v_stmt:=v_stmt||' and not exists( ';
        
        v_stmt:=v_stmt||'select ';--src.source_id as "'||upper(v_columns(1))||'",';
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'bad.'||v_columns(p)||',';
        END LOOP;
        v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
        v_stmt:= v_stmt || ',bad.source_id,bad.system_id from temp.'||v_target_name||'_bad bad ';
     
        v_stmt:= v_stmt || ' where tmp3.'||v_columns(1)||' = bad.'||v_columns(1);
  
        v_stmt:= v_stmt || ') and not exists( select src.source_id as "'||upper(v_columns(1))||'",';
        FOR p IN 2..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'tgt.'||v_columns(p)||', ';
        END LOOP;
        v_stmt:= v_stmt || ' src.source_id,src.system,uniewazniony from dwh.'||v_target_name||' tgt ';
        v_stmt:= v_stmt || ' join dwh.'|| v_target_name||'_key src on src.source_id = tgt.'||v_columns(1);
        v_stmt:= v_stmt || ' where src.id = tmp3.'||v_columns(1)||'))';
        
        dbms_output.put_line(v_stmt);
        EXECUTE IMMEDIATE v_stmt;
        
        v_stmt := '';
        v_join:='';
        
        v_stmt := 'DELETE FROM temp.'||v_target_name||'_bad bb WHERE EXISTS'||
        '(select ';
        
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'bad.'||v_columns(p)||',';
        END LOOP;
            v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
            v_stmt:= v_stmt || ',bad.source_id,bad.system_id,uniewazniony from temp.'||v_target_name||'_bad bad ';
                                   
        FOR rec IN (SELECT ft.nazwa_tabela AS rodzic,wy.nazwa_tabela AS dziecko,wy.nazwa_kolumna_id FROM dwh_meta.rodzic_dziecko wf
                        join dwh_meta.tabela ft ON ft.id = wf.id_rodzic
                        join dwh_meta.tabela wy ON wy.id = wf.id_dziecko
                        WHERE upper(ft.nazwa_tabela) = upper(v_target_name))
        LOOP
           v_join := v_join || 'join dwh.'|| rec.dziecko||' t'||v_iter||' on t'||v_iter||'.source_id = bad.'||rec.nazwa_kolumna_id||' and t'||v_iter||'.system = bad.system_id ';
           v_iter:=v_iter+1;
        END LOOP;
        v_stmt:=v_stmt||v_join;
        v_stmt:= v_stmt || ' where bb.'||v_columns(1)||' = bad.'||v_columns(1)||' ';
        v_stmt:=v_stmt||' and not exists( ';
        
        v_stmt:=v_stmt||'select src.source_id as "'||v_columns(1)||'", ';--src.source_id as "'||upper(v_columns(1))||'",';
        FOR p IN 1..v_columns.count
        LOOP
            v_stmt:= v_stmt ||'tgt.'||v_columns(p)||',';
        END LOOP;
        v_stmt:= substr(v_stmt,0, length(v_stmt)-1);
        v_stmt:= v_stmt || ',src.source_id,src.system from dwh.'||v_target_name||' tgt ';
        
        v_stmt:= v_stmt || ' join dwh.'||v_target_name||'_key src on src.id = tgt.'||v_columns(1);
        v_stmt:= v_stmt || ' join temp.'||v_target_name||'_bad bad on src.id = bad.'||v_columns(1)||' and src.system = bad.system_id';
        
        v_stmt:= v_stmt || ' where src.source_id = bad.'||v_columns(1)||'))';

        
        dbms_output.put_line(v_stmt);
        EXECUTE IMMEDIATE v_stmt;

    END;
    
END hd_gen;

/
