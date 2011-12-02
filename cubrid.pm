#   cubrid.pm
#
#   Copyright (c) 2011 Zhang Hui, China
#
#   See COPYRIGHT section in the documentation below

use 5.008;
use warnings;
use strict;

{   package DBD::cubrid;

    use DBI ();
    use DynaLoader ();
    use vars qw(@ISA $VERSION $err $errstr $sqlstate $drh $dbh);
    @ISA = qw(DynaLoader);

    require_version DBI 1.61;

    $VERSION = '8.4.0';

    bootstrap DBD::cubrid $VERSION;

    $drh = undef;   # holds driver handle once initialized
    $err = 0;       # holds error code for DBI::err
    $errstr = '';   # holds error string for DBI::errstr
    $sqlstate = ''; # holds five character SQLSTATE code

    sub driver {
        return $drh if $drh;

        my($class, $attr) = @_;

        $class .= "::dr";

        $drh = DBI::_new_drh ($class, {
                'Name' => 'cubrid',
                'Version' => $VERSION,
                'Err'    => \$DBD::cubrid::err,
                'Errstr' => \$DBD::cubrid::errstr,
                'Attribution' => 'DBD::cubrid by Zhang Hui'
            });

        $drh
    }
}

{   package DBD::cubrid::dr; # ====== DRIVER ======
    use strict;
    
    sub connect {

        my ($drh, $dsn, $user, $passwd, $attrhash) = @_;
        my %connect_attr;

        if ($dsn =~ /;/) {
            my ($n,$v);
            $dsn =~ s/^\s+//;
            $dsn =~ s/\s+$//;
            $dsn =~ s/^DBI:cubrid://;
            my @dsn = map {
                ($n,$v) = split /\s*=\s*/, $_, -1;
                Carp::carp("DSN component '$_' is not in 'name=value' format")
                    unless defined $v && defined $n;
                (uc($n), $v)
            } split /\s*;\s*/, $dsn;
            my %dsn = @dsn;
            foreach (%dsn) {
                $connect_attr{$_} = $dsn{$_};
            }
        }

        $user = 'public' if not defined $user;

        my ($host, $port, $dbname);

        if ($connect_attr{HOST}) {
            $host = $connect_attr{HOST};
        } else {
            $host = 'localhost';
        }

        if ($connect_attr{PORT}) {
            $port = $connect_attr{PORT};
        } else {
            $port = 33000;
        }

        if ($connect_attr{DATABASE}) {
            $dbname = $connect_attr{DATABASE};
        } else {
            $dbname = 'demodb';
        }

        my $connect_dsn = "cci:CUBRID:$host:$port:$dbname";
        my $is_connect_attr = 0;

        if ($connect_attr{ALTHOSTS}) {
            $connect_dsn .= ":::?alhosts=$connect_attr{ALTHOSTS}";
            $is_connect_attr = 1;
        }

        if ($connect_attr{RCTIME}) {
            if ($is_connect_attr) {
                $connect_dsn .= "&rctime=$connect_attr{RCTIME}";
            } else {
                $connect_dsn .= ":::?rctime=$connect_attr{RCTIME}";
                $is_connect_attr = 1;
            }
        }

        if ($connect_attr{AUTOCOMMIT}) {
           if ($is_connect_attr) {
               $connect_dsn .= "&autocommit=$connect_attr{AUTOCOMMIT}";
           } else {
               $connect_dsn .= ":::?autocommit=$connect_attr{AUTOCOMMIT}";
               $is_connect_attr = 1;
           }
        }

        if ($connect_attr{LOGIN_TIMEOUT}) {
            if ($is_connect_attr) {
                $connect_dsn .= "&login_timeout=$connect_attr{LOGIN_TIMEOUT}";
            } else {
                $connect_dsn .= ":::?login_timeout=$connect_attr{LOGIN_TIMEOUT}";
                $is_connect_attr = 1;
            }
        }

        if ($connect_attr{QUERY_TIMEOUT}) {
            if ($is_connect_attr) {
                $connect_dsn .= "&query_timeout=$connect_attr{QUERY_TIMEOUT}";
            } else {
                $connect_dsn .= ":::?query_timeout=$connect_attr{QUERY_TIMEOUT}";
                $is_connect_attr = 1;
            }
        }

        if ($connect_attr{DISCONNECT_ON_QUERY_TIMEOUT}) {
            if ($is_connect_attr) {
                $connect_dsn .= "&disconnect_on_query_timeout=$connect_attr{DISCONNECT_ON_QUERY_TIMEOUT}";
            } else {
                $connect_dsn .= ":::?disconnect_on_query_timeout=$connect_attr{DISCONNECT_ON_QUERY_TIMEOUT}";
                $is_connect_attr = 1;
            }
        }

        my ($dbh) = DBI::_new_dbh ($drh, {
                'Name' => $dbname,
                'User' => $user,
            });

        DBD::cubrid::db::_login($dbh, $connect_dsn, $user, $passwd, $attrhash) or return undef;

        $dbh
    }

} # end the package of DBD::cubrid::dr


{   package DBD::cubrid::db; # ====== DATABASE ======
    use strict;
    use DBI qw(:sql_types);

    sub prepare {

        my ($dbh, $statement, @attribs) = @_;

        return undef if ! defined $statement;

        my $sth = DBI::_new_sth ($dbh, {
                'Statement' => $statement,
            });

        DBD::cubrid::st::_prepare($sth, $statement, @attribs) or return undef;

        $sth
    }

    sub ping {
        my $dbh = shift;
        local $SIG{__WARN__} = sub { } if $dbh->FETCH('PrintError');
        my $ret = DBD::cubrid::db::_ping($dbh);
        return $ret ? 1 : 0;
    }

    sub get_info {
        my ($dbh, $info_type) = @_;
        require DBD::cubrid::GetInfo;
        my $v = $DBD::cubrid::GetInfo::info{int($info_type)};
        $v = $v->($dbh) if ref $v eq 'CODE';
        return $v;
    }

    sub table_info {
        my ($dbh, $catalog, $schema, $table, $type, $attr) = @_;

        my @names = qw(TABLE_CAT TABLE_SCHEM TABLE_NAME TABLE_TYPE REMARKS);
        my @rows;

        my $sponge = DBI->connect("DBI:Sponge:", '','')
            or return $dbh->DBI::set_err($DBI::err, "DBI::Sponge: $DBI::errstr");

        if ((defined $catalog && $catalog eq "%") &&
             (!defined($schema) || $schema eq "") &&
             (!defined($table) || $table eq ""))
        {
            @rows = (); # Empty, because CUBRID doesn't support catalogs (yet)
        }
        elsif ((defined $schema && $schema eq "%") &&
                (!defined($catalog) || $catalog eq "") &&
                (!defined($table) || $table eq ""))
        {
            @rows = ();
        }
        elsif ((defined $type && $type eq "%") &&
                (!defined($catalog) || $catalog eq "") &&
                (!defined($schema) || $schema eq "") &&
                (!defined($table) || $table eq ""))
        {
            @rows = (
                [ undef, undef, undef, "TABLE", undef ],
                [ undef, undef, undef, "VIEW",  undef ],
            );
        }
        else
        {
            my ($want_tables, $want_views);
            if (defined $type && $type ne "") {
                $want_tables = ($type =~ m/table/i);
                $want_views  = ($type =~ m/view/i);
            }
            else {
                $want_tables = $want_views = 1;
            }

            my $sql = "SELECT class_name, class_type FROM db_class where class_name like '$table'";
            my $sth = $dbh->prepare ($sql) or return undef;
            $sth->execute or return DBI::set_err($dbh, $sth->err(), $sth->errstr());

            while (my $ref = $sth->fetchrow_arrayref()) {
                my $type = (defined $ref->[1] &&
                    $ref->[1] =~ /VCLASS/i) ? 'VIEW' : 'TABLE';
                next if $type eq 'TABLE' && not $want_tables;
                next if $type eq 'VIEW'  && not $want_views;
                push @rows, [ undef, undef, $ref->[0], $type, undef ];
            }
        }

        my $sth = $sponge->prepare("table_info",
            {
                rows          => \@rows,
                NUM_OF_FIELDS => scalar @names,
                NAME          => \@names,
            }
        ) or return $dbh->DBI::set_err($sponge->err(), $sponge->errstr());

        return $sth;
    }

    sub column_info {
        my $dbh = shift;
        my ($catalog, $schema, $table, $column) = @_;

        # ODBC allows a NULL to mean all columns, so we'll accept undef
        $column = '%' unless defined $column;

        my $table_id = $dbh->quote_identifier($table);

        my @names = qw(
            TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_SIZE
            BUFFER_LENGTH DECIMAL_DIGITS NUM_PREC_RADIX NULLABLE REMARKS COLUMN_DEF
            SQL_DATA_TYPE SQL_DATETIME_SUB CHAR_OCTET_LENGTH ORDINAL_POSITION IS_NULLABLE
            CHAR_SET_CAT CHAR_SET_SCHEM CHAR_SET_NAME COLLATION_CAT COLLATION_SCHEM
            COLLATION_NAME UDT_CAT UDT_SCHEM UDT_NAME DOMAIN_CAT DOMAIN_SCHEM DOMAIN_NAME
            SCOPE_CAT SCOPE_SCHEM SCOPE_NAME MAX_CARDINALITY DTD_IDENTIFIER IS_SELF_REF
        );

        my %col_info;

        local $dbh->{FetchHashKeyName} = 'NAME_lc';
        my $desc_sth = $dbh->prepare("SHOW COLUMNS FROM $table_id LIKE " . $dbh->quote($column));
        my $desc = $dbh->selectall_arrayref($desc_sth, { Columns=>{} });

        my $ordinal_pos = 0;
        for my $row (@$desc) {
            my $type = $row->{type};

            my $info = $col_info{ $row->{field} } = {
                TABLE_CAT               => $catalog,
                TABLE_SCHEM             => $schema,
                TABLE_NAME              => $table,
                COLUMN_NAME             => $row->{field},
                NULLABLE                => ($row->{null} eq 'YES') ? 1 : 0,
                IS_NULLABLE             => ($row->{null} eq 'YES') ? "YES" : "NO",
                TYPE_NAME               => uc($type),
                COLUMN_DEF              => $row->{default},
                ORDINAL_POSITION        => ++$ordinal_pos,
            };

            $info->{DATA_TYPE}= $type;
            if ($type =~ /char | varchar | string | nchar/) {
                if ($type =~ /\(/) {
                    my @tmp = split /\(/, $type;
                    $info->{DATA_TYPE} = $tmp[0];
                    my @tmp1 = split /\)/, $tmp[1];
                    $info->{COLUMN_SIZE} = $tmp1[0];
                }
                else {
                    $info->{COLUMN_SIZE} = 4294967295;
                }
            }
            elsif ($type =~ /int | integer/) {
                $info->{COLUMN_SIZE} = 4294967295;
            }
            elsif ($type =~ /smallint | short/) {
                $info->{COLUMN_SIZE} = 65535;
            }
            elsif ($type =~ /bigint/) {
                $info->{COLUMN_SIZE} = 18446744073709551615;
            }
            elsif ($type =~ /numeric/) {
                if ($type =~ /\(/) {
                    my @tmp = split /\(/, $type;
                    $info->{DATA_TYPE} = $tmp[0];
                    my @tmp1 = split /\)/, $tmp[1];
                    my @tmp2 = split /,/, $tmp1[0];
                    $info->{DECIMAL_DIGITS} = $tmp2[0];
                    $info->{NUM_PREC_RADIX} = $tmp2[1];
                }
                else {
                    $info->{DECIMAL_DIGITS} = 15;
                    $info->{NUM_PREC_RADIX} = 0;
                }
            }
            elsif ($type =~ /float/) {
                $info->{DECIMAL_DIGITS} = 38;
            }
            elsif ($type=~ /double/) {
                $info->{DECIMAL_DIGITS} = 15;
            }

            $info->{SQL_DATA_TYPE} ||= $info->{DATA_TYPE};
        }

        my $sponge = DBI->connect("DBI:Sponge:", '','')
            or return $dbh->DBI::set_err($DBI::err, "DBI::Sponge: $DBI::errstr");
        
        my $sth = $sponge->prepare("column_info $table_id", {
                rows          => [ map { [ @{$_}{@names} ] } values %col_info ],
                NUM_OF_FIELDS => scalar @names,
                NAME          => \@names,
            }) or return $dbh->DBI::set_err($sponge->err(), $sponge->errstr());

        return $sth;
    }

    sub primary_key_info {
        my ($dbh, $catalog, $schema, $table) = @_;    

        my @names = qw(
            TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME KEY_SEQ PK_NAME    
        );

        my %col_info;

        local $dbh->{FetchHashKeyName} = 'NAME_lc';
        my $desc_sth = $dbh->prepare("SHOW COLUMNS FROM $table");
        my $desc= $dbh->selectall_arrayref($desc_sth, { Columns=>{} });
        for my $row (@$desc) {
            if ($row->{key} eq 'PRI') {
                $col_info{ $row->{field} }= {
                    TABLE_CAT   => $catalog,
                    TABLE_SCHEM => $schema,
                    TABLE_NAME  => $table,
                    COLUMN_NAME => $row->{field},
                    KEY_SEQ     => 0,
                    PK_NAME     => $row->{key},
                };
            }
        }

        my $sponge = DBI->connect ("DBI:Sponge:", '','')
            or return $dbh->DBI::set_err ($DBI::err, "DBI::Sponge: $DBI::errstr");
        my $sth= $sponge->prepare ("primary_key_info $table", {
                rows          => [ map { [ @{$_}{@names} ] } values %col_info ],
                NUM_OF_FIELDS => scalar @names,
                NAME          => \@names,
            }) or return $dbh->DBI::set_err($sponge->err(), $sponge->errstr());

        return $sth;
    }

    sub foreign_key_info {
        my ($dbh,
            $pk_catalog, $pk_schema, $pk_table,
            $fk_catalog, $fk_schema, $fk_table,
        ) = @_;

        my @names = qw(
            PKTABLE_CAT PKTABLE_SCHEM PKTABLE_NAME PKCOLUMN_NAME
            FKTABLE_CAT FKTABLE_SCHEM FKTABLE_NAME FKCOLUMN_NAME 
            KEY_SEQ  UPDATE_RULE DELETE_RULE FK_NAME PK_NAME DEFERRABILITY
        );

        my %col_info;

        local $dbh->{FetchHashKeyName} = 'NAME_lc';



        my $sponge = DBI->connect ("DBI:Sponge:", '','')
            or return $dbh->DBI::set_err ($DBI::err, "DBI::Sponge: $DBI::errstr");
        my $sth= $sponge->prepare ("foreign_key_info", {
                rows          => [ map { [ @{$_}{@names} ] } values %col_info ],
                NUM_OF_FIELDS => scalar @names,
                NAME          => \@names,
            }) or return $dbh->DBI::set_err($sponge->err(), $sponge->errstr());

        return $sth;
    }

}   # end of package DBD::Oracle::db


{   package DBD::cubrid::st; # ====== STATEMENT ======


}

1;

__END__

=head1 

DBD::cubrid - CUBRID driver for the Perl5 Database Interface (DBI)

=head1 SYNOPSIS

    use DBI;

    $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";
    $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port;autocommit=$autocommit";

    $dbh = DBI->connect ($dsn, $user, $password);
    $sth = $dbh->prepare("SELECT * FROM TABLE");
    $sth->execute;
    $numFields = $sth->{'NUM_OF_FIELDS'};
    $sth->finish;

=head1 EXAMPLE

    #!/usr/bin/perl

    use strict;
    use DBI();

    # Connect to the database.
    my $dbh = DBI->connect (
        "DBI:cubrid:database=demodb;host=localhost;port=33000;autocommit=off",
        "public", "", 
        {'RaiseError' => 1});

    # Drop table 'foo'. This may fail, if 'foo' doesn't exist.
    # Thus we put an eval around it.
    eval { $dbh->do("DROP TABLE foo") };
    print "Dropping foo failed: $@\n" if $@;

    # Create a new table 'foo'. This must not fail, thus we don't
    # catch errors.
    $dbh->do("CREATE TABLE foo (id INTEGER, name VARCHAR(20))");

    # INSERT some data into 'foo'. We are using $dbh->quote() for
    # quoting the name.
    $dbh->do("INSERT INTO foo VALUES (1, " . $dbh->quote("Tim") . ")");

    # Same thing, but using placeholders
    $dbh->do("INSERT INTO foo VALUES (?, ?)", undef, 2, "Jochen");

    # Now retrieve data from the table.
    my $sth = $dbh->prepare("SELECT * FROM foo");
    $sth->execute();
    while (my $ref = $sth->fetchrow_hashref()) {
        print "Found a row: id = $ref->{'id'}, name = $ref->{'name'}\n";
    }
    $sth->finish();

    # Disconnect from the database.
    $dbh->disconnect();

=head1 DESCRIPTION

DBD::cubrid is a Perl module that works with the DBI module to provide access to
CUBRID databases.

=head1 Module Documentation

This documentation describes driver specific behavior and restrictions. It is
not supposed to be used as the only reference for the user. In any case
consult the B<DBI> documentation first!

=for html <a href="http://search.cpan.org/~timb/DBI/DBI.pm">Latest DBI documentation.</a>

=head1 THE DBI CLASS

=head2 DBI Class Methods

=head3 B<connect>

    use DBI;

    $dsn = "DBI:cubrid";
    $dsn = "DBI:cubrid:database=$database";
    $dsn = "DBI:cubrid:database=$database;host=$hostname";
    $dsn = "DBI:cubrid:database=$database;host=$hostname;port=$port";
    $dsn = "DBI:cubrid:database=$database;host=$hostname;port=$port;autocommit=$autocommit"

    $dbh = DBI->connect ($dsn, $user, $password);


This method creates a database handle by connecting to a database, and is the DBI
equivalent of the "new" method. There are some properties you can configure when
create the conntion.

If the HA feature is enabled, you munst specify the connection information of the
standby server, which is used for failover when failure occurs, in the url string
argument of this function.

B<althosts> =standby_broker1_host, standby_broker2_host, . . . : String. Specifies the
broker information of the standby server, which is used for failover when it is 
impossible to connect to the active server. You can specify multiple brokers for failover,
and the connection to the brokers is attempted in the order listed in alhosts.

B<rctime> : INT. An interval between the attempts to connect to the active broker in
which failure occurred. After a failure occurs, the system connects to the broker
specified by althosts (failover), terminates the transaction, and then attempts to
connect to he active broker of the master database at every rctime. The default value
is 600 seconds.

B<autocommit> : String. Configures the auto-commit mode. 
The value maybe true, on, yes, false, off and no.

B<login_timeout> : INT. Configures the login timeout of CUBRID.

B<query_timeout>: INT. Configures the query timeout of CUBRID.

B<disconnect_on_query_timeout> : String. Make the query_timeout effective. 
The value maybe true, on, yes, false, off and no.

=head1 INSTALLATION

=head2 Environment Variables

we will have to install the following software componets in order to usd DBD::cubrid.

=over

=item *

CUBRID DBMS

Install the latest version of CUBRID Database System, and make sure the Environment Variable
%CUBRID% is defined in your system

=item *

Perl Interpreter

If you're new to Perl, you should start by running I<perldoc perlintro> , 
which is a general intro for beginners and provides some background to 
help you navigate the rest of Perl's extensive documentation. Run I<perldoc
perldoc> to learn more things you can do with perldoc.

=item *

DBI module

The DBI is a database access module for the Perl programming language.  It 
defines a set of methods, variables, and conventions that provide a consistent
database interface, independent of the actual database being used.

=back

=head2 Installing with CPAN

To fire up the CPAN module, just get to your command line and run this:

    perl -MCPAN -e shell

If this is the first time you've run CPAN, it's going to ask you a series of 
questions - in most cases the default answer is fine. If you finally receive 
the CPAN prompt, enter

    install DBD::cubrid

=head2 Installing with source code

To build and install from source, you should move into the top-level directory
of the dirstribution and issue the following commands.

    tar zxvf DBD-cubrid-(version)-tar.gz
    cd DBD-cubrid-(version)
    perl Makefile.PL
    make
    make test
    make install

=cut
