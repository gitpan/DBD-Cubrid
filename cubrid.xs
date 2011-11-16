#include "cubrid.h"

MODULE = DBD::cubrid PACKAGE = DBD::cubrid

INCLUDE: cubrid.xsi

MODULE = DBD::cubrid	PACKAGE = DBD::cubrid::dr




MODULE = DBD::cubrid	PACKAGE = DBD::cubrid::db

void
_ping( dbh )
    SV *dbh
    CODE:
    ST(0) = sv_2mortal(newSViv(dbd_db_ping(dbh)));

void
do(dbh, statement, attr=Nullsv, ...)
    SV *dbh
    char *statement
    SV *attr
    PROTOTYPE: $$;$@
    CODE:
{
    int retval;

    if (statement[0] == '\0') {
        XST_mUNDEF(0);
        return;
    }

    if (items < 4) {
        imp_sth_t *imp_sth;
        SV * const sth = dbixst_bounce_method("prepare", 3);
        if (!SvROK(sth))
            XSRETURN_UNDEF;
        imp_sth = (imp_sth_t*)(DBIh_COM(sth));
        retval = dbd_st_execute(sth, imp_sth);
    } else {
        imp_sth_t *imp_sth;
        SV * const sth = dbixst_bounce_method("prepare", 3);
        if (!SvROK(sth))
            XSRETURN_UNDEF;
        imp_sth = (imp_sth_t*)(DBIh_COM(sth));
        if (!dbdxst_bind_params(sth, imp_sth, items-2, ax+2))
            XSRETURN_UNDEF;
        retval = dbd_st_execute(sth, imp_sth);        
    }

    if (retval == 0)
        XST_mPV (0, "0E0");
    else if (retval < -1)
        XST_mUNDEF (0);
    else
        XST_mIV (0, retval);
}

MODULE = DBD::cubrid    PACKAGE = DBD::cubrid::st


