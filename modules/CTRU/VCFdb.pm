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


#------

1;
