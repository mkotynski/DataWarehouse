--------------------------------------------------------
--  DDL for Package HD_GEN
--------------------------------------------------------

  CREATE OR REPLACE NONEDITIONABLE PACKAGE "SYS"."HD_GEN" IS

    TYPE T_COLUMNS IS TABLE OF VARCHAR2(100);
    TYPE T_TAB_COLUMNS IS TABLE OF T_COLUMNS;

    PROCEDURE to_tmp(p_source_table IN VARCHAR2, p_into_table IN VARCHAR2, p_columns IN VARCHAR2, p_id IN VARCHAR2, p_system IN VARCHAR2, p_key_table IN VARCHAR2);
    PROCEDURE TO_DELTA(p_table IN VARCHAR2, p_columns IN VARCHAR2);
    PROCEDURE TO_BAD(p_table IN VARCHAR2, p_columns IN VARCHAR2);

END HD_GEN;

/
