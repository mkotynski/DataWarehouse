--------------------------------------------------------
--  DDL for Package Body HD_COMMON
--------------------------------------------------------

  CREATE OR REPLACE NONEDITIONABLE PACKAGE BODY "SYS"."HD_COMMON" AS 

 procedure drop_stage
 as
 begin
    execute immediate 'drop table stage.csv_klienci';
    execute immediate 'drop table stage.csv_zamowienia';
    execute immediate 'drop table stage.csv_premia';
    execute immediate 'drop table stage.csv_pracownicy';
    execute immediate 'drop table stage.csv_zasilki';
    execute immediate 'drop table stage.csv_zasilki_detale';
    execute immediate 'drop table stage.source2_pracownicy';
    execute immediate 'drop table stage.source2_premia';
    execute immediate 'drop table stage.source2_zasilek';
    execute immediate 'drop table stage.source2_zasilek_detale';
 end;
 
 procedure drop_temp
 as
 begin
 
    execute immediate 'drop table temp.klienci_bad';
    execute immediate 'drop table temp.klienci_delta';
    execute immediate 'drop table temp.klienci_tmp';
    
    execute immediate 'drop table temp.pracownicy_bad';
    execute immediate 'drop table temp.pracownicy_delta';
    execute immediate 'drop table temp.pracownicy_tmp';
    
    execute immediate 'drop table temp.premia_bad';
    execute immediate 'drop table temp.premia_delta';
    execute immediate 'drop table temp.premia_tmp';
    
    execute immediate 'drop table temp.zamowienie_bad';
    execute immediate 'drop table temp.zamowienie_delta';
    execute immediate 'drop table temp.zamowienie_tmp';
    
    execute immediate 'drop table temp.zasilek_bad';
    execute immediate 'drop table temp.zasilek_delta';
    execute immediate 'drop table temp.zasilek_tmp';
    
    execute immediate 'drop table temp.zasilek_detale_bad';
    execute immediate 'drop table temp.zasilek_detale_delta';
    execute immediate 'drop table temp.zasilek_detale_tmp';
 end;


    procedure reset_dwh
    as
    begin
        execute immediate 'truncate table temp.zamowienie_tmp';
        execute immediate 'truncate table temp.zamowienie_delta';
        execute immediate 'truncate table temp.zamowienie_bad';
        execute immediate 'truncate table dwh.zamowienie';
        execute immediate 'truncate table dwh.zamowienie_key';
        
        execute immediate 'truncate table temp.klienci_tmp';
        execute immediate 'truncate table temp.klienci_delta';
        execute immediate 'truncate table temp.klienci_bad';
        execute immediate 'truncate table dwh.klienci';
        execute immediate 'truncate table dwh.klienci_key';
        
        execute immediate 'truncate table temp.pracownicy_tmp';
        execute immediate 'truncate table temp.pracownicy_delta';
        execute immediate 'truncate table temp.pracownicy_bad';
        execute immediate 'truncate table dwh.pracownicy';
        execute immediate 'truncate table dwh.pracownicy_key';
        
        execute immediate 'truncate table temp.premia_tmp';
        execute immediate 'truncate table temp.premia_delta';
        execute immediate 'truncate table temp.premia_bad';
        execute immediate 'truncate table dwh.premia';
        execute immediate 'truncate table dwh.premia_key';
        
        execute immediate 'truncate table temp.zasilek_tmp';
        execute immediate 'truncate table temp.zasilek_delta';
        execute immediate 'truncate table temp.zasilek_bad';
        execute immediate 'truncate table dwh.zasilek';
        execute immediate 'truncate table dwh.zasilek_key';
        
        execute immediate 'truncate table temp.zasilek_detale_tmp';
        execute immediate 'truncate table temp.zasilek_detale_delta';
        execute immediate 'truncate table temp.zasilek_detale_bad';
        execute immediate 'truncate table dwh.zasilek_detale';
        execute immediate 'truncate table dwh.zasilek_detale_key';
        
        execute immediate 'update stage.csv_zamowienia set active = 1';
        execute immediate 'update stage.csv_pracownicy set active = 1';
        execute immediate 'update stage.csv_zasilki set active = 1';
        execute immediate 'update stage.csv_premia set active = 1';
        execute immediate 'update stage.csv_zasilki_detale set active = 1';
        execute immediate 'update stage.source2_pracownicy set active = 1';
        execute immediate 'update stage.source2_premia set active = 1';
        execute immediate 'update stage.source2_zasilek set active = 1';
        execute immediate 'update stage.source2_zasilek_detale set active = 1';
    end;
    
    procedure truncate_stage
    as
    begin
        execute immediate 'truncate table stage.csv_pracownicy';
        execute immediate 'truncate table stage.csv_zamowienia';
        execute immediate 'truncate table stage.csv_zasilki';
        execute immediate 'truncate table stage.csv_zasilki_detale';
        execute immediate 'truncate table stage.csv_premia';
        execute immediate 'truncate table stage.csv_klienci';
         
        execute immediate 'truncate table stage.source2_pracownicy';
        execute immediate 'truncate table stage.source2_premia';
        execute immediate 'truncate table stage.source2_zasilek';
        execute immediate 'truncate table stage.source2_zasilek_detale';
    end;
    
    
     procedure load_pracownicy
     as
     begin
        sys.hd_gen.to_tmp('source2_pracownicy','pracownicy','imie, nazwisko, data_zatrudnienia, data_zwolnienia, pesel', 'id_pracownika','SOURCE2','pracownicy_key');
        sys.hd_gen.to_tmp('csv_pracownicy','pracownicy','imie, nazwisko, data_zatrudnienia, data_zwolnienia, pesel', 'id','csv','pracownicy_key');
        sys.hd_gen.to_delta('pracownicy','id_pracownika,imie,nazwisko,data_zatrudnienia,data_zwolnienia,pesel');
        sys.hd_gen.to_bad('pracownicy','id_pracownika,imie,nazwisko,data_zatrudnienia,data_zwolnienia,pesel');
     end;
     procedure load_klienci
     as
     begin
        sys.hd_gen.to_tmp('csv_klienci','klienci','imie, nazwisko, ulica, nr_mieszkania, poczta, nr_telefonu','id','csv','klienci_key');
        sys.hd_gen.to_delta('klienci','id_klient,imie,nazwisko,ulica,nr_mieszkania,poczta,nr_telefonu');
        sys.hd_gen.to_bad('klienci','id_klient,imie,nazwisko,ulica,nr_mieszkania,poczta,nr_telefonu');
     end;
     procedure load_zasilki
     as
     begin
        sys.hd_gen.to_tmp('source2_zasilek','zasilek',' id_pracownik, id_zasilek_detale', 'id_zasilek','SOURCE2','zasilek_key');
        sys.hd_gen.to_tmp('csv_zasilki','zasilek','id_pracownik, zasilek_detale', 'id','csv','zasilek_key');
        sys.hd_gen.to_delta('zasilek','id_zasilek,id_pracownik,id_zasilek_detale');
        sys.hd_gen.to_bad('zasilek','id_zasilek,id_pracownik,id_zasilek_detale');
     end;
     procedure load_zasilki_detale
     as
     begin
        sys.hd_gen.to_tmp('source2_zasilek_detale','zasilek_detale', 'kwota, data, nazwa', 'id_zasilek_detale','SOURCE2','zasilek_detale_key');
        sys.hd_gen.to_tmp('csv_zasilki_detale','zasilek_detale','kwota, data_przyznania,nazwa', 'id','csv','zasilek_detale_key');
        sys.hd_gen.to_delta('zasilek_detale','id_zasilek_detale,kwota,data,nazwa');
        sys.hd_gen.to_bad('zasilek_detale','id_zasilek_detale,kwota,data,nazwa');
     end;
     procedure load_zamowienia
     as
     begin
        sys.hd_gen.to_tmp('csv_zamowienia','zamowienie','id_klienta, id_pracownika, data_zamowienia', 'id','csv','zamowienie_key');
        sys.hd_gen.to_delta('zamowienie','id_zamowienie,id_klient,id_pracownik,data');  
        sys.hd_gen.to_bad('zamowienie','id_zamowienie,id_klient,id_pracownik,data');
     end;
     procedure load_premia
     as
     begin
        sys.hd_gen.to_tmp('source2_premia','premia','id_pracownik, wysokosc', 'id_premia','SOURCE2','premia_key');
        sys.hd_gen.to_tmp('csv_premia','premia','id_pracownik, premia','id','csv','premia_key');
        sys.hd_gen.to_delta('premia','id_premia,id_pracownik,wysokosc');
        sys.hd_gen.to_bad('premia','id_premia,id_pracownik,wysokosc');
     end;
    
    
    procedure source_to_stage
    as
    begin
        sys.hd.sent_data('SOURCE2','STAGE');

        sys.hd.FROM_CSV_TO_TABLE('csv','klienci', 'klienci.csv');
        sys.hd.FROM_CSV_TO_TABLE('csv','zamowienia', 'zamowienia.csv');
        sys.hd.FROM_CSV_TO_TABLE('csv','pracownicy', 'lista_pracownikow.csv');
        sys.hd.FROM_CSV_TO_TABLE('csv','premia', 'premia.csv');
        sys.hd.FROM_CSV_TO_TABLE('csv','zasilki', 'zasilki.csv');
        sys.hd.FROM_CSV_TO_TABLE('csv','zasilki_detale', 'zasilki_detale.csv');
    end;
    
    
    procedure create_temporary
    as
    begin
        sys.hd.create_temporary('klienci');
        sys.hd.create_temporary('zamowienie');
        sys.hd.create_temporary('pracownicy');
        sys.hd.create_temporary('premia');
        sys.hd.create_temporary('zasilek');
        sys.hd.create_temporary('zasilek_detale');
    end;
END HD_COMMON;

/
