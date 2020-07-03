--------------------------------------------------------
--  DDL for Package HD_ETL
--------------------------------------------------------

  CREATE OR REPLACE NONEDITIONABLE PACKAGE "SYS"."HD_ETL" IS

    TYPE T_COLUMNS IS TABLE OF VARCHAR2(100);
    TYPE T_TAB_COLUMNS IS TABLE OF T_COLUMNS;
    
    PROCEDURE TMP_TO_DW;

   END HD_ETL;

/
