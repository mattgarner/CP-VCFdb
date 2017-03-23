package CTRU::VCFdb;
# 
# 
# 
# 
# Kim Brugger (19 Nov 2013), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use POSIX qw( strftime );


use EASIH::DB;

my $dbi;

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub connect {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  $dbhost  ||= "mgsrv01";
  $db_user ||= 'easih_ro';

  $dbi = EASIH::DB::connect($dbname,$dbhost, $db_user, $db_pass);
  return $dbi;
}


#================== Sample functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_sample {
  my ($name) = @_;

  if ( ! $name ) { 
    print STDERR "add_sample: No sample name provided\n";
    return -1;
  }

  my $sid = fetch_sample_id($name);
  return $sid if ( $sid );

  my %call_hash = ( name => $name);
  return (EASIH::DB::insert($dbi, "sample", \%call_hash));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_id {
  my ( $name ) = @_;
  if ( ! $name ) { 
    print STDERR "fetch_sample_id: No sample name provided\n";
    return -1;
  }
  my $q    = "SELECT sid FROM sample WHERE name = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $name );
  return $line[0] || undef;
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_name {
  my ( $sid ) = @_;

  if ( ! $sid ) { 
    print STDERR "fetch_sample_name: No sample id provided\n";
    return "";
  }

  my $q    = "SELECT name FROM sample WHERE sid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $sid );
  return $line[0] || undef;
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_ids_by_test {
  my ( $test ) = @_;

  if ( ! $test ) { 
    print STDERR "fetch_sample_ids_by_test: No test provided\n";
    return [];
  }

  my $q    = "SELECT sid FROM sample WHERE name like ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_hash( $dbi, $sth, "$test%");
}



# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_sample {
  my ($sid, $name) = @_;

  if ( ! $sid ) { 
    print STDERR "update_sample: No sample id provided\n";
    return "";
  }

  my %call_hash;
  $call_hash{sid}        = $sid  if ($sid);
  $call_hash{name}       = $name if ($name);

  return (EASIH::DB::update($dbi, "sample", \%call_hash, "sid"));
}


#================== Plate functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_plate {
  my ($name) = @_;

  if ( ! $name ) { 
    print STDERR "add_plate: No plate name provided\n";
    return -1;
  }

  my $pid = fetch_plate_id($name);
  return $pid if ( $pid );

  my %call_hash = ( name => $name);
  return (EASIH::DB::insert($dbi, "plate", \%call_hash));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_plate_id {
  my ( $name ) = @_;
  if ( ! $name ) { 
    print STDERR "fetch_plate_id: No plate name provided\n";
    return -1;
  }
  my $q    = "SELECT pid FROM plate WHERE name = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $name );
  return $line[0] || undef;
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_plate_name {
  my ( $pid ) = @_;

  if ( ! $pid ) { 
    print STDERR "fetch_plate_name: No plate id provided\n";
    return "";
  }

  my $q    = "SELECT name FROM plate WHERE pid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $pid );
  return $line[0] || undef;
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_plate {
  my ($pid, $name) = @_;

  if ( ! $pid ) { 
    print STDERR "update_plate: No plate id provided\n";
    return "";
  }

  my %call_hash;
  $call_hash{pid}        = $pid  if ($pid);
  $call_hash{name}       = $name if ($name);

  return (EASIH::DB::update($dbi, "plate", \%call_hash, "pid"));
}

#================== Sample_sequence functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_sample_sequence {
  my ($sid, $pid, $name) = @_;

  if ( ! $sid ) { 
    print STDERR "add_sample_sequence: No sample-id provided\n";
    return -1;
  }

  if ( ! $pid ) { 
    print STDERR "add_sample_sequence: No plate-id provided\n";
    return -2;
  }

  if ( ! $name ) { 
    print STDERR "add_sample_sequence: No sample_sequence name provided\n";
    return -3;
  }

  my $s_name = fetch_sample_name($sid);
  if ( ! $s_name ) {
    print STDERR "add_sample_sequence: Unknown sample-id\n";
    return -4;
  }

  my $p_name = fetch_plate_name($pid);
  if ( ! $p_name ) {
    print STDERR "add_sample_sequence: Unknown plate-id\n";
    return -5;
  }
  
  my $ssid = fetch_sample_sequence_id( $name );
  return $ssid if ( $ssid );
     
  my %call_hash = ( sid  => $sid,
		    pid  => $pid,
		    name => $name);

  return (EASIH::DB::insert($dbi, "sample_sequence", \%call_hash));
}



# 
# 
# 
# Kim Brugger (3 Dec 2013)
sub add_sample_sequence_stats {
  my ($ssid, $total_reads, $mapped_reads, $duplicate_reads, $mean_isize, ) = @_;

  my $ss_name = fetch_sample_sequence_name($ssid);
  if ( ! $ss_name  ) {
    print STDERR "add_sample_variant: Unknown sequence_sample-id $ssid '$ss_name'\n";
    return -6;
  }
     
  my %call_hash = ( ssid => $ssid);
  $call_hash{ total_reads }     = $total_reads     if ( $total_reads );
  $call_hash{ mapped_reads }    = $mapped_reads    if ( $mapped_reads );
  $call_hash{ duplicate_reads } = $duplicate_reads if ( $duplicate_reads );
  $call_hash{ mean_isize }      = $mean_isize      if ( $mean_isize );

  return (EASIH::DB::update($dbi, "sample_sequence", \%call_hash, "ssid"));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_sequence_id {
  my ( $name ) = @_;
  if ( ! $name ) { 
    print STDERR "fetch_sample_sequence_id: No sample_sequence name provided\n";
    return -1;
  }
  my $q    = "SELECT ssid FROM sample_sequence WHERE name = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $name );
  return $line[0] || undef;
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_sequence_name {
  my ( $ssid ) = @_;

  if ( ! $ssid ) { 
    print STDERR "fetch_sample_sequence_name: No sample_sequence id provided\n";
    return "";
  }

  my $q    = "SELECT name FROM sample_sequence WHERE ssid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $ssid );
  return $line[0] || undef;
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_sequence_hash {
  my ( $ssid ) = @_;
  if ( ! $ssid ) { 
    print STDERR "fetch_sample_sequence_hash: No sample_sequence id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM sample_sequence WHERE ssid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_hash( $dbi, $sth, $ssid ));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_sample_sequence {
  my ($ssid, $name, $total_reads, $mapped_reads, $duplicate_reads, $mean_isize, ) = @_;


  if ( ! $ssid ) { 
    print STDERR "update_sample_sequence: No sample sequence id provided\n";
    return -1;
  }

  if ( ! $name ) { 
    print STDERR "update_sample_sequence: No name provided\n";
    return -1;
  }

  my %call_hash;
  $call_hash{ssid}              = $ssid;
  $call_hash{name}              = $name            if ($name);
  $call_hash{ total_reads }     = $total_reads     if ( $total_reads );
  $call_hash{ mapped_reads }    = $mapped_reads    if ( $mapped_reads );
  $call_hash{ duplicate_reads } = $duplicate_reads if ( $duplicate_reads );
  $call_hash{ mean_isize }      = $mean_isize      if ( $mean_isize );
  return (EASIH::DB::update($dbi, "sample_sequence", \%call_hash, "ssid"));
}


#================== region functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_region {
  my ($chr, $start, $end, $name, $reference, $checked) = @_;

  if ( ! $chr ) { 
    print STDERR "add_region: No chr provided\n";
    return -1;
  }

  if ( ! $start ) { 
    print STDERR "add_region: No region start provided\n";
    return -2;
  }

  if ( ! $end ) { 
    print STDERR "add_region: No region end provided\n";
    return -3;
  }

  if ( ! $name ) { 
    print STDERR "add_region: No region name provided\n";
    return -4;
  }

  if ( ! $reference ) { 
    print STDERR "add_region: No reference provided\n";
    return -5;
  }
  
  my $rid = fetch_region_id_by_name( $name );
  if ( $rid ) {
    my $rhash = fetch_region_hash( $rid );
    if ( $$rhash{ chr   }     == $chr &&
	 $$rhash{ start }     == $start &&
	 $$rhash{ end   }     == $end ) {
      return $rid if ( $$rhash{ name  }     eq $name &&
		       $$rhash{ reference } eq $reference );
      return update_region($chr, $start, $end, $name, $reference);
    }
  }

     
  my %call_hash = ( chr       => $chr,
		    start     => $start,
		    end       => $end,
		    name      => $name,
		    reference => $reference);

  $call_hash{ checked } = $checked if ( defined $checked );

  return (EASIH::DB::insert($dbi, "region", \%call_hash));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_region_id_by_name {
  my ( $name ) = @_;

  if ( ! $name ) { 
    print STDERR "fetch_region_id: No region name provided\n";
    return -1;
  }
  my $q    = "SELECT rid FROM region WHERE name = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $name );
  return $line[0] || undef;
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_regions_by_gene {
  my ( $gene ) = @_;

  if ( ! $gene ) { 
    print STDERR "fetch_region_by_gene: No gene name provided\n";
    return -1;
  }
  my $q    = "SELECT * FROM region WHERE name like ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_hash( $dbi, $sth, "$gene%" );
}



# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_region_id_by_position {
  my ( $chr, $pos ) = @_;

  if ( ! $chr || !$pos) { 
    print STDERR "fetch_region_id_by_position: No chromosome or position provided\n";
    return -1;
  }
  my $q    = "SELECT rid FROM region WHERE chr = ? AND start <= ? and end  >= ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $chr, $pos, $pos );
  return $line[0] || undef;
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_region_hash {
  my ( $rid ) = @_;
  if ( ! $rid ) { 
    print STDERR "fetch_region_hash: No region id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM region WHERE rid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_hash( $dbi, $sth, $rid ));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_regions {
  my $q    = "SELECT * FROM region";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_array_hash( $dbi, $sth ));
}



# 
# 
# 
# Kim Brugger (13 Dec 2013)
sub delete_region_n_coverages {
  my ($rid) = @_;

  my $q    = "DELETE FROM region where rid= ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  EASIH::DB::do( $dbi, $sth, $rid );

  $q    = "DELETE FROM coverage where rid= ?";
  $sth  = EASIH::DB::prepare($dbi, $q);
  EASIH::DB::do( $dbi, $sth, $rid );

  
  
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_region {
  my ($rid, $chr, $start, $end, $name, $reference, $checked) = @_;

  if ( ! $rid ) { 
    print STDERR "update_region: No sample sequence id provided\n";
    return -1;
  }

  my %call_hash;
  $call_hash{rid}       = $rid       if ($rid);
  $call_hash{chr}       = $chr       if ($chr);
  $call_hash{start}     = $start     if ($start);
  $call_hash{end}       = $end       if ($end);
  $call_hash{name}      = $name      if ($name);
  $call_hash{checked}   = $checked   if ($checked);
  $call_hash{reference} = $reference if ($reference);

  return (EASIH::DB::update($dbi, "region", \%call_hash, "rid"));
}


#================== variant functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_variant {
  my ($chr, $pos, $ref, $alt, $comment, $annotation) = @_;

  if ( ! $chr ) { 
    print STDERR "add_variant: No chr provided\n";
    return -1;
  }

  if ( ! $pos ) { 
    print STDERR "add_variant: No variant position provided\n";
    return -2;
  }

  if ( ! $ref ) { 
    print STDERR "add_variant: No variant ref base(s) provided\n";
    return -3;
  }

  if ( ! $alt ) { 
    print STDERR "add_variant: No variant alt base(s) provided\n";
    return -4;
  }

  
  my $vid = fetch_variant_id( $chr, $pos, $ref, $alt );
  return $vid if ( $vid );
     
  my %call_hash = ( chr  => $chr,
		    pos  => $pos,
		    ref  => $ref,
		    alt  => $alt);

  $call_hash{ comment   } = $comment     if ( $comment    );
  $call_hash{ annotation } = $annotation if ( $annotation );

  return (EASIH::DB::insert($dbi, "variant", \%call_hash));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_variant_id {
  my ( $chr, $pos, $ref, $alt ) = @_;

  if ( ! $chr || !$pos || !$ref || ! $alt ) { 
    print STDERR "fetch_variant_id: requires 4 paramters: chr, pos, ref and alt\n";
    return -1;
  }

  my $q    = "SELECT vid FROM variant WHERE chr = ? AND pos = ? AND ref = ? AND alt = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $chr, $pos, $ref, $alt );
  return $line[0] || undef;
}



# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_variant_id_by_position {
  my ( $chr, $pos) = @_;

  if ( ! $chr || !$pos ) { 
    print STDERR "fetch_variant_id: requires 2 paramters: chr and pos\n";
    return -1;
  }

  my $q    = "SELECT vid FROM variant WHERE chr = ? AND pos = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_array( $dbi, $sth, $chr, $pos);
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_variants {

  my $q    = "SELECT * FROM variant";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return (EASIH::DB::fetch_array_hash( $dbi, $sth));
}



# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_variant_hash {
  my ( $vid ) = @_;
  if ( ! $vid ) { 
    print STDERR "fetch_variant_hash: No variant id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM variant WHERE vid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_hash( $dbi, $sth, $vid ));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_variant {
  my ($vid, $chr, $pos, $ref, $alt, $comment, $annotation) = @_;

  if ( ! $vid ) { 
    print STDERR "update_variant: No variant id provided\n";
    return -1;
  }

  my %call_hash;
  $call_hash{vid}        = $vid        if ( $vid        );
  $call_hash{chr}        = $chr        if ( $chr        );
  $call_hash{pos}        = $pos        if ( $pos        );
  $call_hash{ref}        = $ref        if ( $ref        );
  $call_hash{alt}        = $alt        if ( $alt        );
  $call_hash{comment}    = $comment    if ( $comment    );
  $call_hash{annotation} = $annotation if ( $annotation );

  return (EASIH::DB::update($dbi, "variant", \%call_hash, "vid"));
}


#================== sample_variant functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_sample_variant {
  my ($ssid, $vid, $depth, $AAF, $quality) = @_;

  if ( ! $ssid ) { 
    print STDERR "add_sample_variant: No sample_sequence id provided\n";
    return -1;
  }

  if ( ! $vid ) { 
    print STDERR "add_sample_variant: No variant id provided\n";
    return -2;
  }

  if ( ! $depth ) { 
    print STDERR "add_sample_variant: No depth provided\n";
    return -3;
  }

  if ( ! $AAF ) { 
    print STDERR "add_sample_variant: No Alternative Allele Freq (AAF) provided\n";
    return -4;
  }

  if ( ! $quality ) { 
    print STDERR "add_sample_variant: No quality provided\n";
    return -5;
  }

  my $ss_name = fetch_sample_sequence_name($ssid);
  if ( ! $ss_name  ) {
    print STDERR "add_sample_variant: Unknown sequence_sample-id $ssid '$ss_name'\n";
    return -6;
  }

  my $v_hash = fetch_variant_hash($vid);
  if ( ! $v_hash || keys %{$v_hash} == 0) {
    print STDERR "add_sample_variant: Unknown variant-id $vid $v_hash\n";
    return -7;
  }

  
  my $sv_hash = fetch_sample_variant_hash( $ssid, $vid );
  return 1 if ( $sv_hash && keys %{$sv_hash} > 0 );

  my %call_hash = ( ssid    => $ssid,
		    vid     => $vid,
		    depth   => $depth,
		    AAF     => $AAF,
		    quality => $quality);


  return (EASIH::DB::insert($dbi, "sample_variant", \%call_hash));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_variant_hash {
  my ($ssid,  $vid ) = @_;
  if ( ! $vid || ! $ssid ) { 
    print STDERR "fetch_sample_variant_hash: No variant and/or sample-sequence id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM sample_variant WHERE ssid = ? AND vid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_hash( $dbi, $sth, $ssid, $vid ));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_variants {
  my ($vid ) = @_;
  if ( ! $vid ) { 
    print STDERR "fetch_sample_variant_hash: No variant and/or sample-sequence id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM sample_variant WHERE vid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_array_hash( $dbi, $sth, $vid ));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_sample_variant {
  my ($ssid, $vid, $depth, $AAF, $quality) = @_;


  if ( ! $ssid ) { 
    print STDERR "add_sample_variant: No sample_sequence id provided\n";
    return -1;
  }

  if ( ! $vid ) { 
    print STDERR "add_sample_variant: No variant id provided\n";
    return -2;
  }

  my $ss_name = fetch_sample_sequence_name($ssid);
  if ( ! $ss_name  ) {
    print STDERR "add_sample_variant: Unknown sequence_sample-id $ssid $ss_name\n";
    return -3;
  }

  my $v_hash = fetch_variant_hash($vid);
  if ( ! $v_hash || keys %{$v_hash} == 0) {
    print STDERR "add_sample_variant: Unknown variant-id $vid $v_hash\n";
    return -4;
  }

  my $sv_hash = fetch_sample_variant_hash( $ssid, $vid );
  if ( !$sv_hash || keys %{$sv_hash} == 0 ) {
    print "update_sample_variant: unknown entry\n";
    return -5;
  }


  my %call_hash;
  $call_hash{ssid}       = $ssid    if ( $ssid    );
  $call_hash{vid}        = $vid     if ( $vid     );
  $call_hash{depth}      = $depth   if ( $depth   );
  $call_hash{AAF}        = $AAF     if ( $AAF     );
  $call_hash{quality}    = $quality if ( $quality );

  return (EASIH::DB::update($dbi, "sample_variant", \%call_hash, "ssid", "vid"));
}


#================== coverage functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_coverage {
  my ($ssid, $rid, $min, $mean, $max, $lows, $missing) = @_;

  if ( ! $ssid ) { 
    print STDERR "add_coverage: No sample_sequence id provided\n";
    return -1;
  }

  if ( ! $rid ) { 
    print STDERR "add_coverage: No region id provided\n";
    return -2;
  }

  if ( ! defined $min ) { 
    print STDERR "add_coverage: No min depth provided\n";
    return -3;
  }

  if ( ! defined  $mean ) { 
    print STDERR "add_coverage: No mean depth provided\n";
    return -4;
  }

  if ( ! defined  $max ) { 
    print STDERR "add_coverage: No max depth provided\n";
    return -5;
  }

  if ( ! defined $lows ) { 
    print STDERR "add_coverage: No low regions provided\n";
    return -6;
  }

  if ( ! defined $missing ) { 
    print STDERR "add_coverage: No missing regions provided\n";
    return -7;
  }


  my $ss_name = fetch_sample_sequence_name( $ssid );
  if ( ! $ss_name  ) {
    print STDERR "add_coverage: Unknown sequence_sample-id: $ssid\n";
    return -8;
  }

  my $r_hash = fetch_region_hash( $rid );
  if ( ! $r_hash || keys %{$r_hash} == 0) {
    print STDERR "add_coverage: Unknown region-id: $rid\n";
    return -9;
  }
  
  my $c_hash = fetch_coverage_hash( $ssid, $rid );
  return 1 if ( $c_hash && keys %{$c_hash} > 0 );

  my %call_hash = ( ssid    => $ssid,
		    rid     => $rid,
		    min     => $min,
		    mean    => $mean,
		    max     => $max,
		    lows    => $lows,
		    missing => $missing);


  return (EASIH::DB::insert($dbi, "coverage", \%call_hash));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_coverage_hash {
  my ($ssid,  $rid ) = @_;
  if ( ! $rid || ! $ssid ) { 
    print STDERR "fetch_coverage_hash: No variant and/or sample-sequence id prorided\n";
    return {};
  }
  my $q    = "SELECT * FROM coverage WHERE ssid = ? AND rid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_hash( $dbi, $sth, $ssid, $rid ));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_coverages_by_rid {
  my ($rid ) = @_;
  if ( ! $rid ) { 
    print STDERR "fetch_coverages_by_rid: No region id prorided\n";
    return {};
  }

  my $q    = "SELECT * FROM coverage WHERE rid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_array_hash( $dbi, $sth, $rid ));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_coverage {
  my ($ssid, $rid, $min, $mean, $max, $lows, $missing) = @_;



  if ( ! $ssid ) { 
    print STDERR "add_coverage: No sample_sequence id provided\n";
    return -1;
  }

  if ( ! $rid ) { 
    print STDERR "add_coverage: No region id provided\n";
    return -2;
  }

  my $ss_name = fetch_sample_sequence_name( $ssid );
  if ( ! $ss_name  ) {
    print STDERR "add_coverage: Unknown sequence_sample-id: $ssid\n";
    return -3;
  }

  my $r_hash = fetch_region_hash( $rid );
  if ( ! $r_hash || keys %{$r_hash} == 0) {
    print STDERR "add_coverage: Unknown region-id: $rid\n";
    return -4;
  }
  
  my %call_hash;
  $call_hash{ ssid }     = $ssid    if ( $ssid            );
  $call_hash{ rid  }     = $rid     if ( $rid             );
  $call_hash{ min  }     = $min     if ( $min             );
  $call_hash{ mean }     = $mean    if ( $mean            );
  $call_hash{ max }      = $max     if ( $max             );
  $call_hash{ lows  }    = $lows    if ( defined $lows    );
  $call_hash{ missing }  = $missing if ( defined $missing );

  return (EASIH::DB::update($dbi, "coverage", \%call_hash, "ssid", "rid"));
}



#================== Complex functions =========================


# 
# 
# 
# Kim Brugger (05 Dec 2013)
sub variants_from_test {
  my ( $test ) = @_;

  if ( ! $test ) { 
    print STDERR "variants_from_test: No test prorided\n";
    return {};
  }

  my $q    = "SELECT vid FROM sample s, sample_sequence ss, sample_variant sv WHERE s.name LIKE ? AND ss.sid = s.sid AND ss.ssid = sv.ssid GROUP BY vid;";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_array_hash( $dbi, $sth, "$test%" ));
}


1;
