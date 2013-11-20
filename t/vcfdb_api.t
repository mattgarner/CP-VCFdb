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

use Test::More tests => 66;

#open( *STDERR, ">/dev/null");

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

my $f_sample_name = CTRU::VCFdb::fetch_sample_name(  );
ok($f_sample_name eq "", "fetch sample name by sample id, no id");

$f_sample_name = CTRU::VCFdb::fetch_sample_name( $sid );
ok($sample_name eq $f_sample_name, "fetch sample name by sample id");

my $f_sample_id = CTRU::VCFdb::fetch_sample_id(  );
ok($f_sample_id eq -1, "fetch sample id by sample name, no name");

$f_sample_id = CTRU::VCFdb::fetch_sample_id( $sample_name );
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

my $f_plate_name = CTRU::VCFdb::fetch_plate_name(  );
ok($f_plate_name eq "", "fetch plate name by plate id, no plate name");

$f_plate_name = CTRU::VCFdb::fetch_plate_name( $pid );
ok($plate_name eq $f_plate_name, "fetch plate name by plate id");

my $f_plate_id = CTRU::VCFdb::fetch_plate_id(  );
ok($f_plate_id == -1, "fetch plate id by plate name, no plate id");

$f_plate_id = CTRU::VCFdb::fetch_plate_id( $plate_name );
ok($pid eq $f_plate_id, "fetch plate id by plate name");

$plate_name = "CP0002";
$f_plate_id = CTRU::VCFdb::update_plate( $pid, $plate_name );
ok($pid eq $f_plate_id, "update plate, persistent id");

$f_plate_name = CTRU::VCFdb::fetch_plate_name( $f_plate_id );
ok($plate_name eq $f_plate_name, "fetched updated plate name by plate id");


# ====================== Sample_sequence ===============================

my $ss_name = "C010001_A";

my $ssid = CTRU::VCFdb::add_sample_sequence();
ok($ssid == -1, "Check for provided sid name when inserting a sample sequence");

$ssid = CTRU::VCFdb::add_sample_sequence($sid, );
ok($ssid == -2, "Check for provided pid name when inserting a sample sequence");

$ssid = CTRU::VCFdb::add_sample_sequence($sid, $pid);
ok($ssid == -3, "Check for provided name when inserting a sample sequence");

$ssid = CTRU::VCFdb::add_sample_sequence(99, $pid, $ss_name);
ok($ssid == -4, "Check for provided valid sid when inserting a sample sequence");

$ssid = CTRU::VCFdb::add_sample_sequence($sid, 99, $ss_name);
ok($ssid == -5, "Check for provided valid pid when inserting a sample sequence");

$ssid = CTRU::VCFdb::add_sample_sequence($sid, $pid, $ss_name);
ok($ssid == 1, "Check for add a sample_sequence with correct parameters");

my $f_ss_name = CTRU::VCFdb::fetch_sample_sequence_name(  );
ok($f_ss_name eq "", "fetch sample sequence name by id, with no id");

$f_ss_name = CTRU::VCFdb::fetch_sample_sequence_name( $ssid );
ok($f_ss_name eq $ss_name, "fetch sample sequence name by id, with an id");

my $f_ssid = CTRU::VCFdb::fetch_sample_sequence_id(  );
ok($f_ssid == -1, "fetch sample sequence id by name, with no id");

$f_ssid = CTRU::VCFdb::fetch_sample_sequence_id( $ss_name );
ok($f_ssid == $ssid, "fetch sample sequence id by name, with an id");

$ss_name = "C010001_ABC";
$f_ssid = CTRU::VCFdb::update_sample_sequence( $ssid, $ss_name );
ok($ssid eq $f_ssid, "update sample_sequence, persistent id");

$f_ss_name = CTRU::VCFdb::fetch_sample_sequence_name( $ssid );
ok($ss_name eq $f_ss_name, "fetched updated sample sequence  name by sample_sequence id");

my $ss_hash = CTRU::VCFdb::fetch_sample_sequence_hash();
ok(!keys %$ss_hash, "fetched sample sequence hash by sample_sequence id, with no id");

$ss_hash = CTRU::VCFdb::fetch_sample_sequence_hash($ssid);
ok($$ss_hash{ssid} == 1 &&
   $$ss_hash{sid} == 1 &&
   $$ss_hash{pid} == 1 &&
   $$ss_hash{name} eq $ss_name, "fetched and checked sample sequence hash by sample_sequence id");


# ====================== REGION ===============================

my $r_name  = "BRCA2_exon2";
my $r_chr   = 13;
my $r_start = 32890598;
my $r_end   = 32890664;

my $rid = CTRU::VCFdb::add_region();
ok($rid == -1, "Check for provided chr when inserting a region");

$rid = CTRU::VCFdb::add_region($r_chr, );
ok($rid == -2, "Check for provided start when inserting a region");

$rid = CTRU::VCFdb::add_region($r_chr, $r_start );
ok($rid == -3, "Check for provided end when inserting a region");

$rid = CTRU::VCFdb::add_region($r_chr, $r_start, $r_end);
ok($rid == -4, "Check for provided name when inserting a region");

$rid = CTRU::VCFdb::add_region($r_chr, $r_start, $r_end, $r_name);
ok($rid == 1, "Check for add a region with correct parameters");
my $r_hash = CTRU::VCFdb::fetch_region_hash($rid);
ok($$r_hash{rid}   == $rid &&
   $$r_hash{name}  eq "BRCA2_exon2" &&
   $$r_hash{chr}   == 13 &&
   $$r_hash{start} == 32890598 &&
   $$r_hash{end}   == 32890664, "fetched and checked region hash by region id (added data)");

my $f_rid = CTRU::VCFdb::fetch_region_id_by_name(  );
ok($f_rid == -1, "fetch region name by id, with no id ");

$f_rid = CTRU::VCFdb::fetch_region_id_by_name( $r_name );
ok($rid eq $f_rid, "fetch region name by id, with an id");

$r_chr   = 14;
$r_start = 32899213;
$r_end   = 32899321;
$r_name  = "BRCA2_exon3";

$f_rid = CTRU::VCFdb::update_region( $rid, $r_chr, $r_start, $r_end, $r_name  );
ok($rid eq $f_rid, "update region, persistent id");

$r_hash = CTRU::VCFdb::fetch_region_hash();
ok(!keys %$r_hash, "fetched region hash by region id, with no id");

$r_hash = CTRU::VCFdb::fetch_region_hash($rid);
ok($$r_hash{rid}   == $rid &&
   $$r_hash{name}  eq "BRCA2_exon3" &&
   $$r_hash{chr}   == 14 &&
   $$r_hash{start} == 32899213 &&
   $$r_hash{end}   == 32899321, "fetched and checked region hash by region id (updated data)");



# ====================== VARIANT ===============================

my $v_chr   = 13;
my $v_pos   = 32890598;
my $v_ref   = "A";
my $v_alt   = "TT";
my $v_comment   = "Common Poly";
my $v_annotation = "BRCA2 mutation!";

my $vid = CTRU::VCFdb::add_variant();
ok($vid == -1, "Check for provided chr when inserting a variant");

$vid = CTRU::VCFdb::add_variant($v_chr, );
ok($vid == -2, "Check for provided position when inserting a variant");

$vid = CTRU::VCFdb::add_variant($v_chr, $v_pos );
ok($vid == -3, "Check for provided reference base(s) when inserting a variant");

$vid = CTRU::VCFdb::add_variant($v_chr, $v_pos, $v_ref);
ok($vid == -4, "Check for provided alternative base(s) when inserting a variant");

$vid = CTRU::VCFdb::add_variant($v_chr, $v_pos, $v_ref, $v_alt, $v_comment, $v_annotation);
ok($vid == 1, "Check for add a variant with correct parameters");
my $v_hash = CTRU::VCFdb::fetch_variant_hash($vid);

ok($$v_hash{vid}        == $vid &&
   $$v_hash{chr}        == $v_chr &&
   $$v_hash{pos}        eq $v_pos &&
   $$v_hash{ref}        eq $v_ref &&
   $$v_hash{alt}        eq $v_alt &&
   $$v_hash{comment}    eq $v_comment &&
   $$v_hash{annotation} eq $v_annotation, "fetched and checked variant hash by variant id (added data)");

my $f_vid = CTRU::VCFdb::fetch_variant_id(  );
ok($f_vid == -1, "fetch variant id, with no chr ");

$f_vid = CTRU::VCFdb::fetch_variant_id( $v_chr, );
ok($f_vid == -1, "fetch variant id, with no position ");

$f_vid = CTRU::VCFdb::fetch_variant_id( $v_chr, $v_pos,  );
ok($f_vid == -1, "fetch variant id, with no reference ");

$f_vid = CTRU::VCFdb::fetch_variant_id( $v_chr, $v_pos, $v_ref, );
ok($f_vid == -1, "fetch variant id, with no alternative ");

$f_vid = CTRU::VCFdb::fetch_variant_id( $v_chr, $v_pos, $v_ref, $v_alt );
ok($vid eq $f_vid, "fetch variant name by id, with an id");

$v_chr   = 15;
$v_pos   = 32890;
$v_ref   = "AAAAAAAAA";
$v_alt   = "GG";
$v_comment   = "Pathogenic";
$v_annotation = "BRCA1 mutation!";


$f_vid = CTRU::VCFdb::update_variant($vid, $v_chr, $v_pos, $v_ref, $v_alt, $v_comment, $v_annotation);
ok($vid eq $f_vid, "update variant, persistent id");

$v_hash = CTRU::VCFdb::fetch_variant_hash();
ok(!keys %$v_hash, "fetched variant hash by variant id, with no id");

$v_hash = CTRU::VCFdb::fetch_variant_hash($vid);

ok($$v_hash{vid}        == $vid &&
   $$v_hash{chr}        == $v_chr &&
   $$v_hash{pos}        eq $v_pos &&
   $$v_hash{ref}        eq $v_ref &&
   $$v_hash{alt}        eq $v_alt &&
   $$v_hash{comment}    eq $v_comment &&
   $$v_hash{annotation} eq $v_annotation, "fetched and checked variant hash by variant id (added data)");



# ====================== SAMPLE_VARIANT ===============================

my $sv_depth   = 99;
my $sv_AAF     = 0.51;
my $sv_quality = 1001;

my $svid = CTRU::VCFdb::add_sample_variant();
ok($svid == -1, "Check for provided sample-sequence id when inserting a sample-variant");

$svid = CTRU::VCFdb::add_sample_variant($ssid, );
ok($svid == -2, "Check for provided variant id when inserting a sample-variant");

$svid = CTRU::VCFdb::add_sample_variant($ssid, $vid);
ok($svid == -3, "Check for provided depth when inserting a sample-variant");

$svid = CTRU::VCFdb::add_sample_variant($ssid, $vid, $sv_depth);
ok($svid == -4, "Check for provided AAF when inserting a sample-variant");

$svid = CTRU::VCFdb::add_sample_variant($ssid, $vid, $sv_depth, $sv_AAF);
ok($svid == -5, "Check for provided quality when inserting a sample-variant");

$svid = CTRU::VCFdb::add_sample_variant($ssid, $vid, $sv_depth, $sv_AAF, $sv_quality);
ok($svid == -100, "Check for add a sample-variant with correct parameters");
my $sv_hash = CTRU::VCFdb::fetch_sample_variant_hash($ssid, $vid);

ok($$sv_hash{ssid}    == $ssid &&
   $$sv_hash{vid}     == $vid &&
   $$sv_hash{depth}   eq $sv_depth &&
   $$sv_hash{AAF}     eq $sv_AAF &&
   $$sv_hash{quality} eq $sv_quality, "fetched and checked sample-variant hash by sample-variant id (added data)");

my $sv_hash = CTRU::VCFdb::fetch_sample_variant_hash();
ok(!keys %$sv_hash, "fetched sample-variant hash by sample-variant id, with no sequence-sample id");

my $sv_hash = CTRU::VCFdb::fetch_sample_variant_hash($ssid);
ok(!keys %$sv_hash, "fetched sample-variant hash by sample-variant id, with no variant id");

$sv_depth   = 999;
$sv_AAF     = 0.11;
$sv_quality = 11;


$f_vid = CTRU::VCFdb::update_sample_variant($ssid, $vid, $sv_depth, $sv_AAF, $sv_quality);
ok($vid eq $f_vid, "update sample-variant, persistent id");

$sv_hash = CTRU::VCFdb::fetch_sample_variant_hash($ssid, $vid);
ok($$sv_hash{ssid}    == $ssid &&
   $$sv_hash{vid}     == $vid &&
   $$sv_hash{depth}   eq $sv_depth &&
   $$sv_hash{AAF}     eq $sv_AAF &&
   $$sv_hash{quality} eq $sv_quality, "fetched and checked sample-variant hash by sample-variant id (updated data)");


