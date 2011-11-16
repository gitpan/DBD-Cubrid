/*
	Copyright (c) 2011 Zhang Hui, China

	See the COPYRIGHT section in the cubrid.pm file for terms.
*/

/* ====== define data types ====== */

/* Driver handle */
struct imp_drh_st {
	dbih_drc_t com;		/* MUST be first element in structure	*/
};


/* Define dbh implementor data structure */
struct imp_dbh_st {
	dbih_dbc_t com;		/* MUST be first element in structure	*/

        int     handle;
};


/* Statement structure */
struct imp_sth_st {
	dbih_stc_t com;		/* MUST be first element in structure	*/

        int     handle;
        int     conn;

        int     col_count;
        int     affected_rows;
        T_CCI_CUBRID_STMT   sql_type;
        T_CCI_COL_INFO      *col_info;
};

/* ------ define functions and external variables ------ */


/* These defines avoid name clashes for multiple statically linked DBD's */

#define dbd_init		cubrid_init
#define dbd_db_disconnect	cubrid_db_disconnect
#define dbd_db_login6		cubrid_db_login6
#define dbd_db_do		cubrid_db_do
#define dbd_db_commit		cubrid_db_commit
#define dbd_db_rollback		cubrid_db_rollback
#define dbd_db_cancel		cubrid_db_cancel
#define dbd_db_destroy		cubrid_db_destroy
#define dbd_db_STORE_attrib	cubrid_db_STORE_attrib
#define dbd_db_FETCH_attrib	cubrid_db_FETCH_attrib
#define dbd_st_prepare		cubrid_st_prepare
#define dbd_st_rows		cubrid_st_rows
#define dbd_st_cancel		cubrid_st_cancel
#define dbd_st_execute		cubrid_st_execute
#define dbd_st_fetch		cubrid_st_fetch
#define dbd_st_finish		cubrid_st_finish
#define dbd_st_destroy		cubrid_st_destroy
#define dbd_st_blob_read	cubrid_st_blob_read
#define dbd_st_STORE_attrib	cubrid_st_STORE_attrib
#define dbd_st_FETCH_attrib	cubrid_st_FETCH_attrib
#define dbd_describe		cubrid_describe
#define dbd_bind_ph		cubrid_bind_ph
#define dbd_db_ping             cubrid_db_ping
#define dbd_db_last_insert_id   cubrid_db_last_insert_id

/* end */

