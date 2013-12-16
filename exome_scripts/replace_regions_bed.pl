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
my $dbname = 'VCFdb_exome';

my %current_exons;

my $dbi = CTRU::VCFdb::connect($dbname, $dbhost, "easih_admin", "easih");
open( my $in, $opts{b}) || die "Could not open '$opts{b}': $!\n";
while(<$in>) {
  next if (/#/);
  next if (/^\z/);
  chomp;
  my ($chr, $start, $end, $name, $ref) = split("\t", $_);
  my $gene = $name;
  $gene =~ s/_exon\d+//i;

  if ( !$current_exons{$gene} ) {
    my @gene_exons = CTRU::VCFdb::fetch_regions_by_gene( $gene );
    foreach my $exon ( @gene_exons) {
      $current_exons{ $gene }{$$exon{chr} }{ $$exon{start} }{ $$exon{end} } = $$exon{ rid };
    }
  }

  if ( $current_exons{ $gene }{$chr }{ $start}{$end} ) {
    my $rid = $current_exons{ $gene }{$chr }{ $start}{ $end };
    print "Should update RID:$rid \n";
    CTRU::VCFdb::update_region($rid, $chr, $start, $end, $name, $ref, 1);
    delete( $current_exons{ $gene }{$chr }{ $start}{$end} );
  }    
  else {
    my $rid = CTRU::VCFdb::add_region($chr, $start, $end, $name, $ref, 1);
    print "Added RID:$rid ($chr, $start, $end, $name, $ref)\n";
  }
  
}

foreach my $gene ( keys %current_exons ) {
  foreach my $chr ( keys %{$current_exons{ $gene }} ) {
    foreach my $start ( keys %{$current_exons{ $gene }{$chr}} ) {
      foreach my $end ( keys %{$current_exons{ $gene }{ $chr}{ $start }} ) {
	print "Should delete: $current_exons{ $gene }{$chr }{ $start}{$end}\n";
	CTRU::VCFdb::delete_region_n_coverages( $current_exons{ $gene }{$chr }{ $start}{$end} );
      }
    }
  }
}

