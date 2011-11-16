/*
   Copyright (c) 2011  Zhang Hui

   See the COPYRIGHT section in the cubrid.pm file for terms.

*/

/* ====== Include CUBRID Header Files ====== */

#include "cas_cci.h"

/* ------ end of CUBRID include files ------ */

//#define NEED_DBIXS_VERSION 93

//#define PERL_NO_GET_CONTEXT

#include <DBIXS.h>		/* installed by the DBI module	*/

#include "dbdimp.h"

#include <dbd_xsh.h>		/* installed by the DBI module	*/

DBISTATE_DECLARE;

/* These prototypes are for dbdimp.c funcs used in the XS file          */
/* These names are #defined to driver specific names in dbdimp.h        */

/* CUBRID types */

#define CUBRID_ER_CANNOT_GET_COLUMN_INFO    -2001
#define CUBRID_ER_CANNOT_FETCH_DATA         -2002



/* end of cubrid.h */

