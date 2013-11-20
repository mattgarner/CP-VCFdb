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
# Kim Brugger (20 Nov 2013)
sub fetch_sample_sequence_id {
  my ( $name ) = @_;
  if ( ! $name ) { 
    print STDERR "fetch_sample_sequence_id: No sample_sequence name provided\n";
    return -1;
  }
  my $q    = "SELECT pid FROM sample_sequence WHERE name = ?";
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

  my $q    = "SELECT name FROM sample_sequence WHERE pid = ?";
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
  my ($ssid, $name) = @_;

  if ( ! $ssid ) { 
    print STDERR "update_sample_sequence: No sample sequence id provided\n";
    return -1;
  }

  if ( ! $name ) { 
    print STDERR "update_sample_sequence: No name provided\n";
    return -1;
  }

  my %call_hash;
  $call_hash{ssid}       = $ssid if ($ssid);
  $call_hash{name}       = $name if ($name);

  return (EASIH::DB::update($dbi, "sample_sequence", \%call_hash, "ssid"));
}


#================== region functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_region {
  my ($chr, $start, $end, $name) = @_;

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
  
  my $rid = fetch_region_id_by_name( $name );
  return $rid if ( $rid );
     
  my %call_hash = ( chr   => $chr,
		    start => $start,
		    end   => $end,
		    name  => $name);

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
sub update_region {
  my ($rid, $chr, $start, $end, $name) = @_;

  if ( ! $rid ) { 
    print STDERR "update_region: No sample sequence id provided\n";
    return -1;
  }

  my %call_hash;
  $call_hash{rid}       = $rid   if ($rid);
  $call_hash{chr}       = $chr   if ($chr);
  $call_hash{start}     = $start if ($start);
  $call_hash{end}       = $end   if ($end);
  $call_hash{name}      = $name  if ($name);

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
    print STDERR "add_sample_variant: Unknown sequence_sample-id $ssid $ss_name\n";
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



1;
