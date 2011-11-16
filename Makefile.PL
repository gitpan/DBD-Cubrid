# Makefile.PL for DBD::cubrid - see README file for more information.
# Copyright (c) 2011 Zhang Hui, China.

use 5.008;
use ExtUtils::MakeMaker;
use Config;
use strict;
require DBI::DBD;

my $os = $^O;
my $sep;

if ($os =~ /linux/i) {
    $sep = "/";
}
else {
    $sep = "\\";
}

my $CUBRID_INCLUDE = $ENV{CUBRID} . $sep . "include";
my $CUBRID_LIB = $ENV{CUBRID} . $sep . "lib";
my $dbi_arch_dir = DBI::DBD::dbd_dbi_arch_dir();

my %opts = (
    'NAME'          => 'DBD::cubrid',
    'VERSION_FROM'  => 'cubrid.pm',
    'INC'           => "-I$CUBRID_INCLUDE -I$dbi_arch_dir",
    'LIBS'          => ["-L$CUBRID_LIB -lcascci"],
    'AUTHOR'        => 'Zhang Hui',
    'ABSTRACT'      => 'CUBRID database driver for the DBI module',
    'dist'          => { 'SUFFIX'   => '.gz',
                         'COMPRESS' => 'gzip -9f' },
    'clean'         => { 'FILES' => '*.xsi' },
    'PREREQ_PM'     => { DBI => '1.61' },
    'OBJECT'        => '$(O_FILES)',
    'LINKTYPE'      => 'dynamic',
);

WriteMakefile(%opts);

package MY;

sub postamble {
    return DBI::DBD::dbd_postamble (@_);
}

sub libscan {
    my ($self, $path) = @_;
    ($path =~ m/\~$/) ? undef : $path;
}
