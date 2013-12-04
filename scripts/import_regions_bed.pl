#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (19 Nov 2013), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

use lib '/software/packages/VCFdb/modules';
use CTRU::VCFdb;

use Getopt::Std;
my $opts = 'b:';
my %opts;
getopts($opts, \%opts);

my $dbhost = 'mgsrv01';
my $dbname = 'VCFdb';

my $dbi = CTRU::VCFdb::connect($dbname, $dbhost, "easih_admin", "easih");
open( my $in, $opts{b}) || die "Could not open '$opts{b}': $!\n";
while(<$in>) {
  next if (/#/);
  next if (/^\z/);
  chomp;
  my ($chr, $start, $end, $name, $ref) = split("\t", $_);
  
  my $rid = CTRU::VCFdb::add_region($chr, $start, $end, $name, $ref);
  print "RID :: $rid\n";
  
}
