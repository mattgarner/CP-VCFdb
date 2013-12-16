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

use Getopt::Std;

my %opts;
getopts("i:", \%opts );

my $infile = shift || usage();
my $sample_name = shift;
#$infile =~ s/^\.\///;
#$infile =~ s/\..*//;
#print "$infile \n";
#exit;
my $cwd      = `pwd`;
chomp($cwd);

my $dbhost = 'mgsrv01';
my $dbname = 'VCFdb_exome';

my $dbi = CTRU::VCFdb::connect($dbname, $dbhost, "easih_admin", "easih");


my $sample_sequence_name = $sample_name;
my $plate_name = 'Unknown';
print " $sample_name => $sample_sequence_name, $plate_name\n";
#exit;

my $sid = CTRU::VCFdb::add_sample( $sample_name );
my $pid = CTRU::VCFdb::add_plate( $plate_name );
my $ssid = CTRU::VCFdb::add_sample_sequence( $sid, $pid, $sample_sequence_name );

#readin_csv("$infile.bam.UKGTN_full");
readin_csv("$infile");
#readin_stats("$infile.bam.flagstat") if ( -e "$infile.bam.flagstat");
#readin_stats("$infile.flagstat")     if ( -e "$infile.flagstat");


# 
# 
# 
# Kim Brugger (04 Dec 2013)
sub readin_stats {
  my ($flagstat_file, $isize_file) = @_;

  my %res;
  open (my $in, $flagstat_file) || die "Could not open '$flagstat_file': $!\n";
  while(<$in>) {
#    print;
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

  # open(  $in, $isize_file) || die "Could not open '$isize_file': $!\n";
  # while(<$in>) {
  #   chomp;
  #   if ( /^MEDIAN_INSERT_SIZE/) {
  #     my @rows = split("\t");
  #     $_ = <$in>;
  #     chomp;
  #     my @fields = split("\t");

  #     for( my $i=0;$i< @rows;$i++) {
  # 	$res{ lc($rows[ $i ])} = $fields[ $i ];
  #     }
  #     last;
  #   }
  # }
  # close( $in );


  print Dumper(\%res);

  
  CTRU::VCFdb::add_sample_sequence_stats( $ssid, $res{total_reads}, $res{mapped_reads}, $res{dup_reads});

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
    next if ( /^Overall/);
    chomp;
    my ($name, $min, $max, $mean, $transcript) = split("\t");
    
    my $rid = CTRU::VCFdb::fetch_region_id_by_name( $name );
    
    if ( !$rid ) {
      print STDERR "could not find region: '$name', skipping on to the next one\n";
      next;
    }
    
    my $cid = CTRU::VCFdb::add_coverage($ssid, $rid, $min, $mean, $max, "","");
  }

  
}


