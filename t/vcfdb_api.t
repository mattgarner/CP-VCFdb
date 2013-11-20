#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (07 Feb 2012), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;


# Sets up dynamic paths for EASIH modules...
# Makes it possible to work with multiple checkouts without setting 
# perllib/perl5lib in the enviroment.
BEGIN {
  my $DYNAMIC_LIB_PATHS = 1;
  if ( $DYNAMIC_LIB_PATHS ) {
    my $path = $0;
    
    if ($path =~ /.*\//) {
      $path =~ s/(.*)\/.*/$1/;
      push @INC, "$path/modules" if ( -e "$path/modules");
      $path =~ s/(.*)\/.*/$1/;
      push @INC, "$path/modules" if ( -e "$path/modules" && ! grep /^$path\/modules/, @INC);
    }
    else {
      push @INC, "../modules" if ( -e "../modules");
      push @INC, "./modules" if ( -e "./modules");
    }
  }
  else {
    push @INC, '/home/kb468/easih-toolbox/modules/';
  }

}


use lib '/software/packages/VCFdb/modules';
use CTRU::VCFdb;
use EASIH::DB;

my $dbhost = 'mgsrv01';

#use EASIH::Misc;
#my $rand_dbname = EASIH::Misc::random_string(20);
my $rand_dbname = "VCFdb_test";
print "Database name :: $rand_dbname\n";

use Test::More tests => 37;

open( *STDERR, ">/dev/null");

print "Drop old test database\n";
EASIH::DB::drop_db($rand_dbname, $dbhost, "easih_admin", "easih");
# Create a random dbase that we can play with...
print "Create test database\n";
EASIH::DB::create_db($rand_dbname, $dbhost, "easih_admin", "easih");
my $dbi_DB = EASIH::DB::connect($rand_dbname, $dbhost, "easih_admin", "easih");
if (-e "sql/tables.sql") {
  EASIH::DB::sql_file($dbi_DB, "sql/tables.sql");
}
elsif (-e "../sql/tables.sql") {
  EASIH::DB::sql_file($dbi_DB, "../sql/tables.sql");
}

my $dbi = CTRU::VCFdb::connect($rand_dbname, $dbhost, "easih_admin", "easih");


my $sid = CTRU::VCFdb::add_sample();
ok($sid == -1, "Check for provided sample name when inserting a sample");

my $sample_name = "C010001";
$sid = CTRU::VCFdb::add_sample( $sample_name );
ok($sid == 1, "Add sample to database");

my $f_sample_name = CTRU::VCFdb::fetch_sample_name( $sid );
ok($sample_name eq $f_sample_name, "fetch sample name by sample id");

my $f_sample_id = CTRU::VCFdb::fetch_sample_id( $sample_name );
ok($sid eq $f_sample_id, "fetch sample id by sample name");

$sample_name = "C010002";
$f_sample_id = CTRU::VCFdb::update_sample( $sid, $sample_name );
ok($sid eq $f_sample_id, "update sample, persistent id");

$f_sample_name = CTRU::VCFdb::fetch_sample_name( $f_sample_id );
ok($sample_name eq $f_sample_name, "fetched updated sample name by sample id");


# ====================== PLATE ===============================


my $pid = CTRU::VCFdb::add_plate();
ok($pid == -1, "Check for provided plate name when inserting a plate");

my $plate_name = "CP0001";
$pid = CTRU::VCFdb::add_plate( $plate_name );
ok($pid == 1, "Add plate to database");

my $f_plate_name = CTRU::VCFdb::fetch_plate_name( $pid );
ok($plate_name eq $f_plate_name, "fetch plate name by plate id");

my $f_plate_id = CTRU::VCFdb::fetch_plate_id( $plate_name );
ok($pid eq $f_plate_id, "fetch plate id by plate name");

$plate_name = "CP0002";
$f_plate_id = CTRU::VCFdb::update_plate( $pid, $plate_name );
ok($pid eq $f_plate_id, "update plate, persistent id");

$f_plate_name = CTRU::VCFdb::fetch_plate_name( $f_plate_id );
ok($plate_name eq $f_plate_name, "fetched updated plate name by plate id");


