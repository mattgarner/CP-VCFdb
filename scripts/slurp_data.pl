#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (04 Dec 2013), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;


use lib '/software/packages/VCFdb/modules';
use CTRU::VCFdb;
use lib "/software/packages/vcftools_0.1.11/lib/perl5/site_perl";
use Vcf;

use Getopt::Std;

my $infile = shift || usage();
$infile =~ s/\..*//;
my $cwd      = `pwd`;
chomp($cwd);

my $dbhost = 'mgsrv01';
my $dbname = 'VCFdb';

my $dbi = CTRU::VCFdb::connect($dbname, $dbhost, "easih_admin", "easih");


my $sample_sequence_name = $infile;
$sample_sequence_name =~ s/.*\///;
my $sample_name = substr($sample_sequence_name, 0, 7);

my $plate_name = 'Unknown';
$plate_name = $1 if ($infile =~ /(CP\d+)/ || $cwd =~ /(CP\d+)/);
print " $sample_name => $sample_sequence_name, $plate_name\n";


my $sid = CTRU::VCFdb::add_sample( $sample_name );
my $pid = CTRU::VCFdb::add_plate( $plate_name );
my $ssid = CTRU::VCFdb::add_sample_sequence( $sid, $pid, $sample_name );

readin_vcf("$infile.vcf");
readin_csv("$infile.var.csv");
readin_stats("$infile.bam.flagstat", "$infile.bam.isize");


# 
# 
# 
# Kim Brugger (04 Dec 2013)
sub readin_stats {
  my ($flagstat_file, $isize_file) = @_;

  my %res;
  open (my $in, $flagstat_file) || die "Could not open '$flagstat_file': $!\n";
  while(<$in>) {
    print;
    if ( /(\d+) .*total/) {
      $res{total_reads} = $1;
    }
    elsif ( /^(\d+) .*duplicates/) {
      $res{dup_reads} = $1;
    }
    elsif ( /(\d+) .*mapped \((.*?)%/) {
      $res{mapped_reads} = $1;
      $res{mapped_perc} = $2;
    }
    elsif ( /(\d+) .*properly paired/) {
      $res{properly_paired} = $1;
    }
    elsif ( /(\d+) .*singletons/) {
      $res{singletons} = $1;
    }
  }
  close( $in );

  open(  $in, $isize_file) || die "Could not open '$isize_file': $!\n";
  while(<$in>) {
    chomp;
    if ( /^MEDIAN_INSERT_SIZE/) {
      my @rows = split("\t");
      $_ = <$in>;
      chomp;
      my @fields = split("\t");

      for( my $i=0;$i< @rows;$i++) {
	$res{ lc($rows[ $i ])} = $fields[ $i ];
      }
      last;
    }
  }
  close( $in );


#  print Dumper(\%res);

  
  CTRU::VCFdb::add_sample_sequence_stats( $ssid, $res{total_reads}, $res{mapped_reads}, $res{dup_reads}, $res{mean_insert_size});

}




# 
# 
# 
# Kim Brugger (04 Dec 2013)
sub readin_csv {
  my ($file ) = @_;
  
  if ( ! -e $file ) {
    print STDERR "$file does not exist!\n";
    return;
  }

  open( my $in, $file) || die "Could not open '$file': $!\n";
  while(<$in>) {
    next if (!/#  capture:/);
    chomp;
    s/#  capture: //;
    my ($name, $min, $max, $mean, $lows, $missing, $transcript) = split("\t");
    
    print "  my ($name, $min, $max, $mean, $lows, $missing, $transcript) = \n";

    my $rid = CTRU::VCFdb::fetch_region_id_by_name( $name );
    
    if ( !$rid ) {
      print STDERR "could not find region: '$name', skipping on to the next one\n";
      next;
    }
    
    my $cid = CTRU::VCFdb::add_coverage($ssid, $rid, $min, $mean, $max, $lows, $missing);
  }

  
}



# 
# 
# 
# Kim Brugger (04 Dec 2013)
sub readin_vcf {
  my ($file ) = @_;
  
  if ( ! -e $file ) {
    print STDERR "$file does not exist!\n";
    return;
  }
  
  my $vcf = Vcf->new(file=>$file);
  $vcf->parse_header();
  
  while (my $entry = $vcf->next_data_hash()) {
  
    foreach my $alt ( @{$$entry{ ALT}}) {
      my $vid = CTRU::VCFdb::add_variant($$entry{CHROM}, $$entry{POS}, $$entry{REF}, $alt);

      print "VID :: $vid\n";

      my ($ref_freq, $alt_freq) = split(",", $$entry{gtypes}{$sample_sequence_name}{AD});
      my $AAF = $alt_freq/($ref_freq+$alt_freq);
      CTRU::VCFdb::add_sample_variant($ssid, $vid, $$entry{gtypes}{$sample_sequence_name}{DP}, $AAF, $$entry{QUAL});

    }
  }
}
