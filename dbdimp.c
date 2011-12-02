/*
	dbdimp.c
	Copyright (c) 2011 Zhang Hui, China
*/

#include "cubrid.h"

#define CUBRID_ER_MSG_LEN 1024
#define CUBRID_BUFFER_LEN 1024

static struct _error_message {
    int err_code;
    char *err_msg;
} cubrid_err_msgs[] = {
    {CUBRID_ER_CANNOT_GET_COLUMN_INFO, "Cannot get column info"},
    {CUBRID_ER_CANNOT_FETCH_DATA, "Cannot fetch data"},
    {0, ""}
};

/***************************************************************************
 * 
 * Name:    dbd_init
 * 
 * Purpose: Called when the driver is installed by DBI
 *
 * Input:   dbistate - pointer to the DBIS variable, used for some
 *                     DBI internal things
 *
 * Returns: Nothing
 *
 **************************************************************************/

void
dbd_init( dbistate_t *dbistate )
{
    DBIS = dbistate;
    cci_init();
}

/***************************************************************************
 * 
 * Name:    dbd_discon_all
 * 
 * Purpose: Disconnect all database handles at shutdown time
 *
 * Input:   dbh - database handle being disconnecte
 *          imp_dbh - drivers private database handle data
 * 
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_discon_all( SV *drh, imp_drh_t *imp_drh )
{
    cci_end ();

    if (!PL_dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
        sv_setiv(DBIc_ERR(imp_drh), (IV)1);
        sv_setpv(DBIc_ERRSTR(imp_drh), (char*)"disconnect_all not implemented");
        return FALSE;
    }

    return FALSE;
}


/***************************************************************************
 * 
 * Name:    handle_error
 * 
 * Purpose: Called to associate an error code and an error message 
 *          to some handle
 *
 * Input:   h - the handle in error condition
 *          e - the error code
 *          error - the error message
 *
 * Returns: Nothing
 *
 **************************************************************************/

static int
get_error_msg( int err_code, char *err_msg )
{
    int i;

    for (i = 0; ; i++) {
        if (!cubrid_err_msgs[i].err_code)
            break;
        if (cubrid_err_msgs[i].err_code == err_code) {
            snprintf (err_msg, CUBRID_ER_MSG_LEN, 
                        "ERROR: CLIENT, %d, %s", 
                        err_code, cubrid_err_msgs[i].err_msg);
            return 0;
        }
    }
    return -1;
}

static void
handle_error( SV* h, int e, T_CCI_ERROR *error )
{
    D_imp_xxh (h);
    char msg[CUBRID_ER_MSG_LEN] = {'\0'};
    SV *errstr;

    if (DBIc_TRACE_LEVEL (imp_xxh) >= 2)
        PerlIO_printf (DBILOGFP, "\t\t--> handle_error\n");

    errstr = DBIc_ERRSTR (imp_xxh);
    sv_setiv (DBIc_ERR (imp_xxh), (IV)e);

    if (e < -2000) {
        get_error_msg (e, msg);
    } else if (cci_get_error_msg (e, error, msg, CUBRID_ER_MSG_LEN) < 0) {
        snprintf (msg, CUBRID_ER_MSG_LEN, "Unknown Error");
    }     

    sv_setpv (errstr, msg);

    if (DBIc_TRACE_LEVEL (imp_xxh) >= 2)
        PerlIO_printf (DBIc_LOGPIO (imp_xxh), "%s\n", msg);

    if (DBIc_TRACE_LEVEL (imp_xxh) >= 2)
        PerlIO_printf (DBILOGFP, "\t\t<-- handle_error\n");

    return;
}

/***************************************************************************
 * 
 * Name:    dbd_db_login6
 * 
 * Purpose: Called for connecting to a database and logging in.
 *
 * Input:   dbh - database handle being initialized
 *          imp_dbh - drivers private database handle data
 *          dbname - the database we want to log into; may be like
 *                   "dbname:host" or "dbname:host:port"
 *          user - user name to connect as
 *          password - passwort to connect with
 *
 * Returns: Nothing
 *
 **************************************************************************/

int
dbd_db_login6( SV *dbh, imp_dbh_t *imp_dbh,
        char *dbname, char *uid, char *pwd, SV *attr )
{
    int  con, res;
    T_CCI_ERROR error;

    if ((con = cci_connect_with_url (dbname, uid, pwd)) < 0) {
        handle_error (dbh, con, NULL);
        return FALSE;
    }

    imp_dbh->handle = con;

    if ((res = cci_end_tran (con, CCI_TRAN_COMMIT, &error)) < 0) {
        handle_error (dbh, res, &error);
        return FALSE;
    }

    DBIc_IMPSET_on(imp_dbh);
    DBIc_ACTIVE_on(imp_dbh);

    return TRUE;
}

/***************************************************************************
 *
 * Name:    dbd_db_commit
 *          dbd_db_rollback
 *
 * Purpose: Commit/Rollback any pending transaction to the database.
 *
 * Input:   dbh - database handle being disconnected
 *          imp_dbh - drivers private database handle data
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

static int
_dbd_db_end_tran( SV *dbh, imp_dbh_t *imp_dbh, int type )
{
    int res;
    T_CCI_ERROR error;

    if ((res = cci_end_tran (imp_dbh->handle, type, &error)) < 0) {
        handle_error (dbh, res, &error);
        return FALSE;
    }
    return TRUE;
}

int
dbd_db_commit( SV *dbh, imp_dbh_t *imp_dbh )
{
    return _dbd_db_end_tran (dbh, imp_dbh, CCI_TRAN_COMMIT);
}

int
dbd_db_rollback( SV *dbh, imp_dbh_t *imp_dbh )
{
    return _dbd_db_end_tran (dbh, imp_dbh, CCI_TRAN_ROLLBACK);
}

/***************************************************************************
 *
 * Name:    dbd_db_destroy
 *
 * Purpose: Our part of the dbh destructor
 *
 * Input:   dbh - database handle being disconnected
 *          imp_dbh - drivers private database handle data
 *
 * Returns: Nothing
 *
 **************************************************************************/

void
dbd_db_destroy( SV *dbh, imp_dbh_t *imp_dbh )
{
    if (DBIc_ACTIVE(imp_dbh)) {
        (void)dbd_db_disconnect(dbh, imp_dbh);
    }

    DBIc_IMPSET_off(imp_dbh);
}

/***************************************************************************
 *
 * Name:    dbd_db_disconnect
 *
 * Purpose: Disconnect a database handle from its database
 *
 * Input:   dbh - database handle being disconnected
 *          imp_dbh - drivers private database handle data
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_db_disconnect( SV *dbh, imp_dbh_t *imp_dbh )
{
    int res;
    T_CCI_ERROR error;

    DBIc_ACTIVE_off(imp_dbh);

    if ((res = cci_disconnect (imp_dbh->handle, &error)) < 0) {
        handle_error (dbh, res, &error);
        return FALSE;
    }

    return TRUE;
}

/***************************************************************************
 *
 * Name:    dbd_db_STORE_attrib
 *
 * Purpose: Function for storing dbh attributes
 *
 * Input:   dbh - database handle being disconnected
 *          imp_dbh - drivers private database handle data
 *          keysv - the attribute name
 *          valuesv - the attribute value
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_db_STORE_attrib( SV *dbh, imp_dbh_t *imp_dbh, SV *keysv, SV *valuesv )
{
    STRLEN kl;
    char *key = SvPV (keysv,kl);
    int on = SvTRUE (valuesv);

    switch (kl) {
    case 10:
        if (strEQ("AutoCommit", key)) {
            if (on) {
                cci_set_autocommit (imp_dbh->handle, CCI_AUTOCOMMIT_TRUE);
            }
            else {
                cci_set_autocommit (imp_dbh->handle, CCI_AUTOCOMMIT_FALSE);
            }

            DBIc_set (imp_dbh, DBIcf_AutoCommit, on);
        }
        break;
    }

    return TRUE;
}

/***************************************************************************
 *
 * Name:    dbd_db_FETCH_attrib
 *
 * Purpose: Function for fetching dbh attributes
 *
 * Input:   dbh - database handle being disconnected
 *          imp_dbh - drivers private database handle data
 *          keysv - the attribute name
 *          valuesv - the attribute value
 *
 * Returns: An SV*, if sucessfull; NULL otherwise
 *
 **************************************************************************/

SV * 
dbd_db_FETCH_attrib(SV *dbh, imp_dbh_t *imp_dbh, SV *keysv)
{
    STRLEN kl;
    char *key = SvPV (keysv, kl);
    SV *retsv = Nullsv;
    
    switch (kl) {
    case 10:
        if (strEQ("AutoCommit", key)) {
            retsv = boolSV(DBIc_has(imp_dbh,DBIcf_AutoCommit));
        }
        break;
    }
    return sv_2mortal(retsv);
}

/******************************************************************************/
SV *
dbd_db_last_insert_id( SV *dbh, imp_dbh_t *imp_dbh,
        SV *catalog, SV *schema, SV *table, SV *field, SV *attr )
{
    SV *sv;
    char *name = NULL;
    int res;
    T_CCI_ERROR error;

    if ((res = cci_last_insert_id (imp_dbh->handle, &name, &error))) {
        handle_error (dbh, res, &error);
        return Nullsv;
    }

    if (!name) {
        return Nullsv;
    } else {
        sv = newSVpvn (name, strlen(name));
        free (name);
    }

    return sv_2mortal (sv);
}

/******************************************************************************/
int
dbd_db_ping( SV *dbh )
{
    int res;
    T_CCI_ERROR error;
    char *query = "SELECT 1+1 from db_root";
    int req_handle = 0, result = 0, ind = 0;

    D_imp_dbh (dbh);

    if ((res = cci_prepare (imp_dbh->handle, query, 0, &error)) < 0) {
        handle_error (dbh, res, &error);
        return FALSE;
    }

    req_handle = res;

    if ((res = cci_execute (req_handle, 0, 0, &error)) < 0) {
        handle_error (dbh, res, &error);
        return FALSE;
    }

    while (1) {
        res = cci_cursor (req_handle, 1, CCI_CURSOR_CURRENT, &error);
        if (res == CCI_ER_NO_MORE_DATA) {
            break;
        }
        if (res < 0) {
            handle_error (dbh, res, &error);
            return FALSE;
        }

        if ((res = cci_fetch (req_handle, &error)) < 0) {
            handle_error (dbh, res, &error);
            return FALSE;
        }

        if ((res = cci_get_data (req_handle, 1, CCI_A_TYPE_INT, &result, &ind)) < 0) {
            handle_error (dbh, res, &error);
            return FALSE;
        }

        if (result == 2) {
            cci_close_req_handle (req_handle);
            return TRUE;
        }
    }

    cci_close_req_handle (req_handle);
    return FALSE;
}

/***************************************************************************
 *
 * Name:    dbd_st_prepare
 *
 * Purpose: Called for preparing an SQL statement; our part of the
 *          statement handle constructor
 *
 * Input:   sth - statement handle being initialized
 *          imp_sth - drivers private statement handle data
 *          statement - pointer to string with SQL statement
 *          attribs - statement attributes, currently not in use
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_st_prepare( SV *sth, imp_sth_t *imp_sth, char *statement, SV *attribs )
{
    int res;
    T_CCI_ERROR error;

    D_imp_dbh_from_sth;

    imp_sth->conn = imp_dbh->handle;

    imp_sth->col_count = -1;
    imp_sth->sql_type = 0;

    if ((res = cci_prepare (imp_sth->conn, statement, 0, &error)) < 0) {
        handle_error (sth, res, &error);
        return FALSE;
    }

    imp_sth->handle = res;

    DBIc_NUM_PARAMS(imp_sth) = cci_get_bind_num (res);

    DBIc_IMPSET_on(imp_sth);

    return TRUE;
}

/***************************************************************************
 *
 * Name:    dbd_st_execute
 *
 * Purpose: Called for execute the prepared SQL statement; our part of
 *          the statement handle constructor
 *
 * Input:   sth - statement handle being initialized
 *          imp_sth - drivers private statement handle data
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_st_execute( SV *sth, imp_sth_t *imp_sth )
{
    int res, option = 0, max_col_size = 0;
    T_CCI_ERROR error;
    T_CCI_COL_INFO *col_info;
    T_CCI_SQLX_CMD sql_type;
    int col_count;

    if ((res = cci_execute (imp_sth->handle, option, 
                    max_col_size, &error)) < 0) {
        handle_error (sth, res, &error);
        return -2;
    }

    col_info = cci_get_result_info (imp_sth->handle, &sql_type, &col_count);
    if (sql_type == SQLX_CMD_SELECT && !col_info) {
        handle_error(sth, CUBRID_ER_CANNOT_GET_COLUMN_INFO, NULL);
        return -2;
    }

    imp_sth->col_info = col_info;
    imp_sth->sql_type = sql_type;
    imp_sth->col_count = col_count;

    switch (sql_type) {
    case SQLX_CMD_INSERT:
    case SQLX_CMD_UPDATE:
    case SQLX_CMD_DELETE:
    case SQLX_CMD_CALL:
        imp_sth->affected_rows = res;
        break;
    case SQLX_CMD_SELECT:
    default:
        imp_sth->affected_rows = -1;
    }

    if (sql_type == SQLX_CMD_SELECT) {
        res = cci_cursor (imp_sth->handle, 1, CCI_CURSOR_CURRENT, &error);
        if (res < 0 && res != CCI_ER_NO_MORE_DATA) {
            handle_error (sth, res, &error);
            return -2;
        }

        DBIc_NUM_FIELDS (imp_sth) = col_count;
        DBIc_ACTIVE_on (imp_sth);
    }
    
    return imp_sth->affected_rows;
}

/***************************************************************************
 *
 * Name:    dbd_st_fetch
 *
 * Purpose: Called for execute the prepared SQL statement; our part of
 *          the statement handle constructor
 *
 * Input:   sth - statement handle being initialized
 *          imp_sth - drivers private statement handle data
 *
 * Returns: array of columns; the array is allocated by DBI via
 *          DBIS->get_fbav(imp_sth), even the values of the array
 *          are prepared, we just need to modify them appropriately
 *
 **************************************************************************/

static AV *
_dbd_st_fetch_row( SV *sth, imp_sth_t *imp_sth)
{
    int i, res, type, num, ind;
    char *buf;
    double ddata;
    AV *av;

    av = DBIS->get_fbav(imp_sth);

    for (i = 0; i < imp_sth->col_count; i++) {
        SV *sv = AvARRAY(av)[i];

        type = CCI_GET_RESULT_INFO_TYPE (imp_sth->col_info, i+1);
        
        switch (type) {
        case CCI_U_TYPE_INT:
        case CCI_U_TYPE_SHORT:
            if ((res = cci_get_data (imp_sth->handle, 
                            i+1, CCI_A_TYPE_INT, &num, &ind)) < 0) {
                handle_error (sth, res, NULL);
                return Nullav;
            }
            if (ind < 0) {
                (void) SvOK_off (sv);
            } else {
                sv_setiv (sv, num);
            }
            break;
        case CCI_U_TYPE_FLOAT:
        case CCI_U_TYPE_DOUBLE:
            if ((res = cci_get_data (imp_sth->handle,
                            i+1, CCI_A_TYPE_DOUBLE, &ddata, &ind)) < 0) {
                handle_error (sth, res, NULL);
                return Nullav;
            }
            if (ind < 0) {
                (void) SvOK_off (sv);
            } else {
                sv_setuv (sv, ddata);
            }
            break;
        default:
            if ((res = cci_get_data (imp_sth->handle,
                            i+1, CCI_A_TYPE_STR, &buf, &ind)) < 0) {
                handle_error (sth, res, NULL);
                return FALSE;
            }
            if (ind < 0) {
                (void) SvOK_off (sv);
            } else {
                sv_setpvn (sv, buf, strlen(buf));
            }
        }
    }

    return av;
}

AV *
dbd_st_fetch( SV *sth, imp_sth_t *imp_sth )
{
    AV *av;
    int res;
    T_CCI_ERROR error;

    if (DBIc_ACTIVE(imp_sth)) {
        DBIc_ACTIVE_off(imp_sth);
    }

    res = cci_cursor (imp_sth->handle, 0, CCI_CURSOR_CURRENT, &error);
    if (res == CCI_ER_NO_MORE_DATA) {
        return Nullav;
    } else if (res < 0) {
        handle_error (sth, res, &error);
        return Nullav;
    }

    if ((res = cci_fetch (imp_sth->handle, &error)) < 0) {
        handle_error (sth, res, &error);
        return Nullav;
    }

    av = _dbd_st_fetch_row (sth, imp_sth);

    res = cci_cursor (imp_sth->handle, 1, CCI_CURSOR_CURRENT, &error);
    if (res < 0 && res != CCI_ER_NO_MORE_DATA) {
        handle_error (sth, res, &error);
        return Nullav;
    }

    return av;
}

/***************************************************************************
 *
 * Name:    dbd_st_finish
 *
 * Purpose: Called for freeing a CUBRID result
 *
 * Input:   sth - statement handle being initialized
 *          imp_sth - drivers private statement handle data
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_st_finish( SV *sth, imp_sth_t *imp_sth )
{
    if (!DBIc_ACTIVE(imp_sth))
        return TRUE;

    DBIc_ACTIVE_off(imp_sth);

    return TRUE;
}

/***************************************************************************
 *
 * Name:    dbd_st_destroy
 *
 * Purpose: Our part of the statement handles destructor
 *
 * Input:   sth - statement handle being initialized
 *          imp_sth - drivers private statement handle data
 *
 * Returns: Nothing
 *
 **************************************************************************/

void
dbd_st_destroy( SV *sth, imp_sth_t *imp_sth )
{
    if (imp_sth->handle) {
        cci_close_req_handle (imp_sth->handle);
        imp_sth->handle = 0;

        imp_sth->col_count = -1;
        imp_sth->sql_type = 0;
        imp_sth->affected_rows = -1;
    }

    DBIc_IMPSET_off(imp_sth);

    return;
}

/***************************************************************************
 *
 * Name:    dbd_st_STORE_attrib
 *
 * Purpose: Modifies a statement handles attributes
 *
 * Input:   sth - statement handle being initialized
 *          imp_sth - drivers private statement handle data
 *          keysv - attribute name
 *          valuesv - attribute value
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_st_STORE_attrib( SV *sth, imp_sth_t *imp_sth, SV *keysv, SV *valuesv )
{
    return TRUE;
}

/***************************************************************************
 *
 * Name:    dbd_st_FETCH_attrib
 *
 * Purpose: Retrieves a statement handles attributes
 *
 * Input:   sth - statement handle being initialized
 *          imp_sth - drivers private statement handle data
 *          keysv - attribute name
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

SV *
dbd_st_FETCH_attrib( SV *sth, imp_sth_t *imp_sth, SV *keysv )
{
    SV *retsv = Nullsv;
    STRLEN kl;
    char *key = SvPV (keysv, kl);
    int i;
    char col_name[128] = {'\0'};

    switch (kl) {
    case 4:
        if (strEQ ("NAME", key)) {
            AV *av = newAV ();
            retsv = newRV_inc (sv_2mortal ((SV *)av));
            for (i = 1; i<= imp_sth->col_count; i++) {
                strcpy (col_name, CCI_GET_RESULT_INFO_NAME (imp_sth->col_info, i));
                av_store (av, i-1, newSVpv (col_name, 0));
            }
        }
        else if (strEQ("TYPE", key)) {
            int type;
            AV *av = newAV ();
            retsv = newRV_inc (sv_2mortal ((SV *)av));
            for (i = 1; i<= imp_sth->col_count; i++) {
                type = CCI_GET_RESULT_INFO_TYPE (imp_sth->col_info, i);
                av_store (av, i-1, newSViv (type));
            }
        }
        break;
    case 5:
        if (strEQ ("SCALE", key)) {
            AV *av = newAV ();
            int scale;
            retsv = newRV_inc (sv_2mortal ((SV *)av));
            for (i = 1; i<= imp_sth->col_count; i++) {
                scale = CCI_GET_RESULT_INFO_SCALE (imp_sth->col_info, i);
                av_store (av, i-1, newSViv (scale));
            }
        }
        break;
    case 8:
        if (strEQ ("NULLABLE", key)) {
            AV *av = newAV ();
            int not_null;
            retsv = newRV_inc (sv_2mortal ((SV *)av));
            for (i = 1; i<= imp_sth->col_count; i++) {
                not_null = CCI_GET_RESULT_INFO_PRECISION (imp_sth->col_info, i);
                av_store (av, i-1, newSViv (not_null));
            }
        }
        break;
    case 9:
        if (strEQ ("PRECISION", key)) {
            AV *av = newAV ();
            int precision;
            retsv = newRV_inc (sv_2mortal ((SV *)av));
            for (i = 1; i<= imp_sth->col_count; i++) {
                precision = CCI_GET_RESULT_INFO_PRECISION (imp_sth->col_info, i);
                av_store (av, i-1, newSViv (precision));
            }
        }
        break;
    }

    return sv_2mortal (retsv);
}

/***************************************************************************
 *
 * Name:    dbd_st_blob_read
 *
 * Purpose: Used for blob reads if the statement handles blob 
 *
 * Input:   sth - statement handle from which a blob will be 
 *                  fetched (currently not supported by DBD::cubrid)
 *          imp_sth - drivers private statement handle data
 *          field - field number of the blob
 *          offset - the offset of the field, where to start reading
 *          len - maximum number of bytes to read
 *          destrv - RV* that tells us where to store
 *          destoffset - destination offset
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_st_blob_read(SV *sth, imp_sth_t *imp_sth,
        int field, long offset, long len, SV *destrv, long destoffset)
{
    sth = sth;
    imp_sth = imp_sth;
    field = field;
    offset = offset;
    len = len;
    destrv = destrv;
    destoffset = destoffset;
    return FALSE;
}

/**************************************************************************/

int
dbd_st_rows( SV * sth, imp_sth_t * imp_sth )
{
    return imp_sth->affected_rows;
}

/***************************************************************************
 *
 * Name:    dbd_bind_ph
 *
 * Purpose: Binds a statement value to a parameter
 *
 * Input:   sth - statement handle being initialized
 *          imp_sth - drivers private statement handle data
 *          param - parameter number, counting starts with 1
 *          value - value being inserted for parameter "param"
 *          sql_type - SQL type of the value
 *          attribs - bind parameter attributes
 *          inout - TRUE, if parameter is an output variable (currently
 *                  this is not supported)
 *          maxlen - ???
 *
 * Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

int
dbd_bind_ph( SV *sth, imp_sth_t *imp_sth, SV *param, SV *value,
             IV sql_type, SV *attribs, int is_inout, IV maxlen )
{
    int res;
    char *bind_value = NULL;
    STRLEN bind_value_len;

    int index = SvIV(param);
    if (index < 1 || index > DBIc_NUM_PARAMS(imp_sth)) {
        handle_error (sth, CCI_ER_BIND_INDEX, NULL);
        return FALSE;
    }

    bind_value = SvPV (value, bind_value_len);

    if ((res = cci_bind_param (imp_sth->handle, index, CCI_A_TYPE_STR,
                    bind_value, CCI_U_TYPE_CHAR, 0)) < 0) {
        handle_error(sth, res, NULL);
        return FALSE;
    }

    return TRUE;
}

