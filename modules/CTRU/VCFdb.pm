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



1;
