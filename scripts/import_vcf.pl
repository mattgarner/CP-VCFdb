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
use lib "/software/packages/vcftools_0.1.11/lib/perl5/site_perl";
use Vcf;

use Getopt::Std;
my $opts = 'tv:RG:e:X:';
my %opts;
getopts($opts, \%opts);


my $dbhost = 'mgsrv01';
my $dbname = 'VCFdb';

my $dbi = CTRU::VCFdb::connect($dbname, $dbhost, "easih_admin", "easih");

my $vcf_file = $opts{v} || usage();

my $vcf = Vcf->new(file=>$vcf_file);
$vcf->parse_header();

my $sample_name;
my $sample_sequence_name;
my $plate_name = 'CP0054';

$sample_sequence_name = $vcf_file;
$sample_sequence_name =~ s/.*\///;
$sample_sequence_name =~ s/\.vcf.*//;

$sample_name = substr($sample_sequence_name, 0, 7);

print " $sample_name => $sample_sequence_name\n";

my $sid = CTRU::VCFdb::add_sample( $sample_name );
my $pid = CTRU::VCFdb::add_plate( $plate_name );
my $ssid = CTRU::VCFdb::add_sample_sequence( $sid, $pid, $sample_name );

print "$sid,$pid->$ssid\n";

while (my $entry = $vcf->next_data_hash()) {
  
  foreach my $alt ( @{$$entry{ ALT}}) {
    my $vid = CTRU::VCFdb::add_variant($$entry{CHROM}, $$entry{POS}, $$entry{REF}, $alt);

    print "VID :: $vid\n";

    my ($ref_freq, $alt_freq) = split(",", $$entry{gtypes}{$sample_sequence_name}{AD});
    my $AAF = $alt_freq/($ref_freq+$alt_freq);
    CTRU::VCFdb::add_sample_variant($ssid, $vid, $$entry{gtypes}{$sample_sequence_name}{DP}, $AAF, $$entry{QUAL});

  }

}
