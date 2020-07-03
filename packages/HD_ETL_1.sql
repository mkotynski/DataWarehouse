--------------------------------------------------------
--  DDL for Package Body HD_ETL
--------------------------------------------------------

  CREATE OR REPLACE NONEDITIONABLE PACKAGE BODY "SYS"."HD_ETL" AS
    
    --- TMP TO DATAWAREHOUSE
    ---------------------------------------------------------------------------------------------------    
    
    PROCEDURE tmp_to_dw
    AS
    ncurs SYS_REFCURSOR;
    b temp.zamowienie_delta%ROWTYPE;
    c temp.klienci_delta%ROWTYPE;
    pracownik temp.pracownicy_delta%ROWTYPE;
    premia temp.premia_delta%ROWTYPE;
    zasilek temp.zasilek_delta%ROWTYPE;
    zasilekd temp.zasilek_detale_delta%ROWTYPE;
    v_stmt VARCHAR2(2000):='';
    v_cnt NUMBER;
    v_id NUMBER := 0;
    v_exists number:=0;
    
    v_id_pracownik number;
    v_id_klient number;
    v_id_zasile_detale number;
    BEGIN
    
    ------------------------------------------------------------------
    ----------------------- KLIENCI (WYMIAR) -------------------------
    ------------------------------------------------------------------
    v_stmt := 'select distinct id_klient,imie,nazwisko,ulica,nr_mieszkania,poczta,nr_telefonu, start_dttm, end_dttm, source_id, system_id,uniewazniony from temp.klienci_delta';
        OPEN ncurs FOR v_stmt;
        LOOP
            FETCH ncurs INTO c;
            EXIT WHEN ncurs%NOTFOUND;
            EXECUTE IMMEDIATE 'select count(*) from dwh.klienci zam '||
            'join dwh.klienci_key zk2 on zk2.id = id_klient '||
            'join temp.klienci_delta tmp on zk2.source_id = '''||c.id_klient||''' and zk2.system = '''||c.system_id||''' '||
             ' where zam.id_klient <> '''||c.id_klient||''' or zam.imie <> '''||c.imie||''' or zam.nazwisko <> '''||c.nazwisko||''' or zam.ulica <> '''||c.ulica||''' or zam.nr_mieszkania <> '''
            ||c.nr_mieszkania||''' or zam.poczta <> '''||c.poczta||''' or zam.nr_telefonu <> '''||c.nr_telefonu||'''' INTO v_cnt;
            
            BEGIN
            SELECT id INTO v_id FROM dwh.klienci_key WHERE source_id = c.id_klient and system = c.system_id;
            EXCEPTION WHEN no_data_found THEN
                v_id := 0;
            END;
            
            BEGIN
            SELECT count(id) INTO v_exists FROM dwh.klienci
            join dwh.klienci_key kk on kk.source_id = c.id_klient and kk.system = c.system_id
            WHERE id_klient = kk.id and imie = c.imie and nazwisko = c.nazwisko 
            and ulica = c.ulica and nr_mieszkania = c.nr_mieszkania and poczta = c.poczta and nr_telefonu = c.nr_telefonu;
            EXCEPTION WHEN no_data_found THEN
                v_exists := 0;
            END;
            
            IF c.uniewazniony = 't' THEN
                 UPDATE dwh.klienci SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_klient = v_id;
            ELSE
                IF v_exists = 0 THEN
                    IF v_cnt > 0 THEN
                        UPDATE dwh.klienci SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_klient = v_id;
                    END IF;
                    INSERT INTO dwh.klienci
                    VALUES (sys.hd.Generete_dataw_id(c.source_id, c.system_id, 'dwh.klienci_key'),c.imie, c.nazwisko, c.ulica,c.nr_mieszkania,c.poczta,c.nr_telefonu, SYSDATE, To_date('31-12-9999','dd-mm-yyyy'));
                    END IF;
                END IF;
        END LOOP;
        v_stmt :='';
          
    ------------------------------------------------------------------
    ----------------------- PRACOWNICY (WYMIAR) -------------------------
    ------------------------------------------------------------------
    v_stmt := 'select distinct id_pracownika, imie, nazwisko, data_zatrudnienia, data_zwolnienia, pesel, start_dttm, end_dttm, source_id, system_id,uniewazniony from temp.pracownicy_delta';
        OPEN ncurs FOR v_stmt;
        LOOP
            FETCH ncurs INTO pracownik;
            EXIT WHEN ncurs%NOTFOUND;
            EXECUTE IMMEDIATE 'select count(*) from dwh.pracownicy zam '||
            'join dwh.pracownicy_key zk2 on zk2.id = id_pracownika '||
            'join temp.pracownicy_delta tmp on zk2.source_id = '''||pracownik.id_pracownika||''' and zk2.system = '''||pracownik.system_id||''' '||
            ' where zam.id_pracownika <> '''||pracownik.id_pracownika||''' or zam.imie <> '''||pracownik.imie||''' or zam.nazwisko <> '''||pracownik.nazwisko||''' or zam.data_zatrudnienia <> to_date('''
            ||pracownik.data_zatrudnienia||''') or zam.data_zwolnienia <> to_date('''||pracownik.data_zwolnienia||''') or zam.pesel <> '''||pracownik.pesel||'''' INTO v_cnt;
            
            BEGIN
            SELECT id INTO v_id FROM dwh.pracownicy_key WHERE source_id = pracownik.id_pracownika and system = pracownik.system_id;
            EXCEPTION WHEN no_data_found THEN
                v_id := 0;
            END;
             
            BEGIN
            SELECT count(id) INTO v_exists FROM dwh.pracownicy
            join dwh.pracownicy_key kk on kk.source_id = pracownik.id_pracownika and kk.system = pracownik.system_id
            WHERE id_pracownika = kk.id and imie = pracownik.imie and nazwisko = pracownik.nazwisko 
            and data_zatrudnienia = pracownik.data_zatrudnienia and data_zwolnienia = pracownik.data_zwolnienia and pesel = pracownik.pesel;
            EXCEPTION WHEN no_data_found THEN
                v_exists := 0;
            END; 
             
            IF pracownik.uniewazniony = 't' THEN
                 UPDATE dwh.pracownicy SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_pracownika = v_id;
            ELSE
                IF v_exists = 0 THEN
                    IF v_cnt > 0 THEN
                        UPDATE dwh.pracownicy SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_pracownika = v_id;
                    END IF;
                    INSERT INTO dwh.pracownicy
                    VALUES (sys.hd.Generete_dataw_id(pracownik.source_id, pracownik.system_id, 'dwh.pracownicy_key'),pracownik.imie, pracownik.nazwisko,pracownik.data_zatrudnienia, pracownik.data_zwolnienia, pracownik.pesel, SYSDATE, To_date('31-12-9999','dd-mm-yyyy'));
                END IF;
            END IF;
        END LOOP;
        v_stmt :='';
        
    ------------------------------------------------------------------
    ----------------------- PREMIA (FAKT) -------------------------
    ------------------------------------------------------------------
    v_stmt := 'select distinct id_premia, id_pracownik, wysokosc, start_dttm, end_dttm, source_id, system_id,uniewazniony from temp.premia_delta';
        OPEN ncurs FOR v_stmt;
        LOOP
            FETCH ncurs INTO premia;
            EXIT WHEN ncurs%NOTFOUND;
            EXECUTE IMMEDIATE 'select count(*) from dwh.premia zam '||
            'join dwh.premia_key zk2 on zk2.id = id_premia '||
            'join temp.premia_delta tmp on zk2.source_id = '''||premia.id_premia||''' and zk2.system = '''||premia.system_id||'''' INTO v_cnt;
            
            select id into v_id_pracownik from DWH.pracownicy_key where source_id = premia.id_pracownik and system = premia.system_id;
            
            BEGIN
            SELECT id INTO v_id FROM dwh.premia_key WHERE source_id = premia.id_premia and system = premia.system_id;
            EXCEPTION WHEN no_data_found THEN
                v_id := 0;
            END;
            
--            v_exists := 0;
--            BEGIN
--            SELECT count(id) INTO v_exists FROM dwh.premia
--            join dwh.premia_key kk on kk.source_id = premia.id_premia and kk.system = premia.system_id
--            WHERE id_premia = kk.id and id_pracownik = v_id_pracownik and wysokosc = premia.wysokosc;
--            dbms_output.put_line('exists:'||v_exists);
--            EXCEPTION WHEN no_data_found THEN
--                v_exists := 0;
--            END; 
--            
            EXECUTE IMMEDIATE 'SELECT count(id)FROM dwh.premia '||
            'join dwh.premia_key kk on kk.source_id = '||premia.id_premia||' and kk.system = '''||premia.system_id||
            ''' WHERE id_premia = kk.id and id_pracownik = '||v_id_pracownik||' and wysokosc = '||premia.wysokosc INTO v_exists;
            
            dbms_output.put_line('SELECT count(id) FROM dwh.premia
            join dwh.premia_key kk on kk.source_id = '||premia.id_premia||' and kk.system ='''||premia.system_id||''' WHERE id_premia = kk.id and id_pracownik = '||v_id_pracownik||' and wysokosc = '||premia.wysokosc||'');
                  
            IF premia.uniewazniony = 't' THEN
                 UPDATE dwh.premia SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_premia = v_id;
            ELSE
            dbms_output.put_line('exists:'||v_exists);
                IF v_exists = 0 THEN
                dbms_output.put_line('nie istnieje identyczny');
                    IF v_cnt > 0 THEN
                        UPDATE dwh.premia SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_premia = v_id;
                        dbms_output.put_line('update ujemy');
                    END IF;
                    INSERT INTO dwh.premia
                    VALUES (sys.hd.Generete_dataw_id(premia.source_id, premia.system_id, 'dwh.premia_key'), v_id_pracownik, premia.wysokosc, SYSDATE, To_date('31-12-9999','dd-mm-yyyy'));
                END IF;
                dbms_output.put_line('istnieje identyczny');
            END IF;
        END LOOP;
        v_stmt :='';
    
    
        ------------------------------------------------------------------
        ----------------------- ZAMOWIENIA (FAKT) ------------------------
        ------------------------------------------------------------------
        v_stmt := 'select distinct id_zamowienie, id_klient, id_pracownik, data, start_dttm, end_dttm, source_id, system_id,uniewazniony from temp.zamowienie_delta';
        OPEN ncurs FOR v_stmt;
        LOOP
            FETCH ncurs INTO b;
            EXIT WHEN ncurs%NOTFOUND;
            EXECUTE IMMEDIATE 'select count(*) from dwh.zamowienie zam '||
            'join dwh.zamowienie_key zk2 on zk2.id = id_zamowienie '||
            'join temp.zamowienie_delta tmp on zk2.source_id = '''||b.id_zamowienie||''' and zk2.system = '''||b.system_id||''''||
            ' where zam.id_zamowienie <> '''||b.id_zamowienie||''' or zam.id_klient <> '''||b.id_klient||''' or zam.id_pracownik <> '''||b.id_pracownik||''' '||' or zam.data <> to_date('''||b.data||''')' INTO v_cnt;
            
            select id into v_id_pracownik from DWH.pracownicy_key where source_id = b.id_pracownik and system = b.system_id;
            select id into v_id_klient from DWH.klienci_key where source_id = b.id_klient and system = b.system_id;
            
            BEGIN
            SELECT id INTO v_id FROM dwh.zamowienie_key WHERE source_id = b.id_zamowienie and system = b.system_id;
            EXCEPTION WHEN no_data_found THEN
                v_id := 0;
            END;
            
            BEGIN
            SELECT count(id) INTO v_exists FROM dwh.zamowienie
            join dwh.zamowienie_key kk on kk.source_id = b.id_zamowienie and kk.system = b.system_id
            WHERE id_zamowienie = kk.id and id_klient = v_id_klient and id_pracownik = v_id_pracownik and data = b.data;
            EXCEPTION WHEN no_data_found THEN
                v_exists := 0;
            END; 
            
            -- PRZYPADEK W KTORYM TRZEBA ZAMKNAC DATE (REKORD uniewazniony)
            IF b.uniewazniony = 't' THEN
                 UPDATE dwh.zamowienie SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_zamowienie = v_id;
            ELSE
                --  JEZELI JEST TO AKTUALIZACJA ISTNIEJACEJ DANEJ (ZAMYKAMY DATE POPRZEDNIEGO)
                IF v_exists = 0 THEN
                    IF v_cnt > 0 THEN
                        UPDATE dwh.zamowienie SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_zamowienie = v_id;
                    END IF;
                    -- WSTAWIANIE NOWEGO REKORDU DO DOCELOWEJ TABELI
                    INSERT INTO dwh.zamowienie
                    VALUES (sys.hd.Generete_dataw_id(b.source_id, b.system_id, 'dwh.zamowienie_key'),v_id_klient, v_id_pracownik, b.data, SYSDATE, To_date('31-12-9999','dd-mm-yyyy'));
                END IF;
            END IF;
        END LOOP;
        
        ------------------------------------------------------------------
        ----------------------- ZASILEK (FAKT) ------------------------
        ------------------------------------------------------------------
        v_stmt := 'select distinct id_zasilek, id_pracownik, id_zasilek_detale, start_dttm, end_dttm, source_id, system_id,uniewazniony from temp.zasilek_delta';
        OPEN ncurs FOR v_stmt;
        LOOP
            FETCH ncurs INTO zasilek;
            EXIT WHEN ncurs%NOTFOUND;
            EXECUTE IMMEDIATE 'select count(*) from dwh.zasilek zam '||
            'join dwh.zasilek_key zk2 on zk2.id = id_zasilek '||
            'join temp.zasilek_delta tmp on zk2.source_id = '''||zasilek.id_zasilek||''' and zk2.system = '''||zasilek.system_id||'''' INTO v_cnt;
            
            select id into v_id_pracownik from DWH.pracownicy_key where source_id = zasilek.id_pracownik and system = zasilek.system_id;
            select id into v_id_zasile_detale from DWH.zasilek_detale_key where source_id = zasilek.id_zasilek_detale and system = zasilek.system_id;

            BEGIN
            SELECT id INTO v_id FROM dwh.zasilek_key WHERE source_id = zasilek.id_zasilek;
            EXCEPTION WHEN no_data_found THEN
                v_id := 0;
            END;
            
            BEGIN
            SELECT count(id) INTO v_exists FROM dwh.zasilek
            join dwh.zasilek_key kk on kk.source_id = zasilek.id_zasilek and kk.system = zasilek.system_id
            WHERE id_zasilek = kk.id and id_pracownik = v_id_pracownik and id_zasilek_detale = v_id_zasile_detale;
            EXCEPTION WHEN no_data_found THEN
                v_exists := 0;
            END; 
            
            -- PRZYPADEK W KTORYM TRZEBA ZAMKNAC DATE (REKORD uniewazniony)
            IF zasilek.uniewazniony = 't' THEN
                 UPDATE dwh.zasilek SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_zasilek = v_id;
            ELSE
                --  JEZELI JEST TO AKTUALIZACJA ISTNIEJACEJ DANEJ (ZAMYKAMY DATE POPRZEDNIEGO)
                  --select id into v_id_klient from DWH.klienci_key where source_id = b.id_klient;
                IF v_exists = 0 THEN
                    IF v_cnt > 0 THEN
                        UPDATE dwh.zasilek SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_zasilek = v_id;
                    END IF;
                    -- WSTAWIANIE NOWEGO REKORDU DO DOCELOWEJ TABELI
                    INSERT INTO dwh.zasilek
                    VALUES (sys.hd.Generete_dataw_id(zasilek.source_id, zasilek.system_id, 'dwh.zasilek_key'), v_id_pracownik, v_id_zasile_detale, SYSDATE, To_date('31-12-9999','dd-mm-yyyy'));
                END IF; 
            END IF;
        END LOOP;
        
         ------------------------------------------------------------------
        ----------------------- ZASILEK_DETALE (WYMIAR) ------------------------
        ------------------------------------------------------------------
        v_stmt := 'select distinct id_zasilek_detale, kwota, data, nazwa, start_dttm, end_dttm, source_id, system_id,uniewazniony from temp.zasilek_detale_delta';
        OPEN ncurs FOR v_stmt;
        LOOP
            FETCH ncurs INTO zasilekd;
            EXIT WHEN ncurs%NOTFOUND;
            EXECUTE IMMEDIATE 'select count(*) from dwh.zasilek_detale zam '||
            'join dwh.zasilek_detale_key zk2 on zk2.id = id_zasilek_detale '||
            'join temp.zasilek_detale_delta tmp on zk2.source_id = '''||zasilekd.id_zasilek_detale||''' and zk2.system = '''||zasilekd.system_id||'''' 
            INTO v_cnt;
            
            BEGIN
            SELECT id INTO v_id FROM dwh.zasilek_key WHERE source_id = zasilekd.id_zasilek_detale and system = zasilekd.system_id;
            EXCEPTION WHEN no_data_found THEN
                v_id := 0;
            END;
            
            BEGIN
            SELECT count(id) INTO v_exists FROM dwh.zasilek_detale
            join dwh.zasilek_detale_key kk on kk.source_id = zasilekd.id_zasilek_detale and kk.system = zasilekd.system_id
            WHERE id_zasilek_detale = kk.id and kwota = zasilekd.kwota and nazwa = zasilekd.nazwa;
            EXCEPTION WHEN no_data_found THEN
                v_exists := 0;
            END; 
            
            -- PRZYPADEK W KTORYM TRZEBA ZAMKNAC DATE (REKORD uniewazniony)
            IF zasilekd.uniewazniony = 't' THEN
                 UPDATE dwh.zasilek_detale SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_zasilek_detale = v_id;
            ELSE
                --  JEZELI JEST TO AKTUALIZACJA ISTNIEJACEJ DANEJ (ZAMYKAMY DATE POPRZEDNIEGO)
                  --select id into v_id_klient from DWH.klienci_key where source_id = b.id_klient;
                IF v_exists = 0 THEN
                    IF v_cnt > 0 THEN
                        UPDATE dwh.zasilek_detale SET end_dttm = To_date(SYSDATE,'YYYY/MM/DD HH24:MI') WHERE end_dttm >= To_date('31-12-9999','DD-MM-YYYY') AND end_dttm <= To_date('31-12-9999','DD-MM-YYYY') AND id_zasilek_detale = v_id;
                    END IF;
                    -- WSTAWIANIE NOWEGO REKORDU DO DOCELOWEJ TABELI
                    INSERT INTO dwh.zasilek_detale
                    VALUES (sys.hd.Generete_dataw_id(zasilekd.source_id, zasilekd.system_id, 'dwh.zasilek_detale_key'), zasilekd.kwota, zasilekd.data, zasilekd.nazwa, SYSDATE, To_date('31-12-9999','dd-mm-yyyy'));
                END IF; 
            END IF;
        END LOOP;
        
        -- CZYSZCZENIE TYMCZASOWYCH TABEL PO PRZEPROWADZENIU PROCESU ETL
        EXECUTE IMMEDIATE 'truncate table temp.zamowienie_tmp';
        EXECUTE IMMEDIATE 'truncate table temp.zamowienie_delta';
        
        EXECUTE IMMEDIATE 'truncate table temp.klienci_tmp';
        EXECUTE IMMEDIATE 'truncate table temp.klienci_delta';
        
        EXECUTE IMMEDIATE 'truncate table temp.pracownicy_tmp';
        EXECUTE IMMEDIATE 'truncate table temp.pracownicy_delta';
        
        EXECUTE IMMEDIATE 'truncate table temp.premia_tmp';
        EXECUTE IMMEDIATE 'truncate table temp.premia_delta';
        
        EXECUTE IMMEDIATE 'truncate table temp.zasilek_tmp';
        EXECUTE IMMEDIATE 'truncate table temp.zasilek_delta';
        
        EXECUTE IMMEDIATE 'truncate table temp.zasilek_detale_tmp';
        EXECUTE IMMEDIATE 'truncate table temp.zasilek_detale_delta';
    END;

END;

/
