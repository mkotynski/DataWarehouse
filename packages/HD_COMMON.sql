--------------------------------------------------------
--  DDL for Package HD_COMMON
--------------------------------------------------------

  CREATE OR REPLACE NONEDITIONABLE PACKAGE "SYS"."HD_COMMON" AS 

 procedure drop_stage;
 procedure drop_temp;
 procedure reset_dwh;
 procedure truncate_stage;
 
 procedure load_pracownicy;
 procedure load_klienci;
 procedure load_zasilki;
 procedure load_zasilki_detale;
 procedure load_zamowienia;
 procedure load_premia;
 
 procedure source_to_stage;
 
 procedure create_temporary;
END HD_COMMON;

/
