#!perl -w
# vim: ft=perl

use Test::More;
use DBI;
use strict;
use lib 't', '.';
require 'lib.pl';
$|= 1;

use vars qw($table $test_dsn $test_user $test_passwd);
my $dbh;
eval {$dbh= DBI->connect($test_dsn, $test_user, $test_passwd,
                      { RaiseError => 1, PrintError => 1, AutoCommit => 0 });};

if ($@) {
    plan skip_all => "ERROR: $DBI::errstr. Can't continue test";
}

plan tests => 7; 

#
# test primary_key_info ()
#

ok(defined $dbh, "Connected to database for key info tests");

ok($dbh->do("DROP TABLE IF EXISTS $table"), "Dropped table");

# Non-primary key is there as a regression test for Bug #26786.
ok($dbh->do("CREATE TABLE $table (a int, b varchar(20), c int,
                                primary key (a,b), key (c))"),
   "Created table $table");

my $sth= $dbh->primary_key_info(undef, undef, $table);
ok($sth, "Got primary key info");

my $key_info= $sth->fetchall_arrayref;

my $expect= [
              [ undef, undef, $table, 'a', 0, 'PRI' ],
              [ undef, undef, $table, 'b', 0, 'PRI' ],
            ];
is_deeply($key_info, $expect, "Check primary_key_info results");

is_deeply([ $dbh->primary_key(undef, undef, $table) ], [ 'a', 'b' ],
          "Check primary_key results");

ok($dbh->do("DROP TABLE $table"), "Dropped table");

$dbh->disconnect();
