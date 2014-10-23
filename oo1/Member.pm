package Member;

use strict;

use lib qw( /amiperl);

use Data::Dumper;
use Table;

our @ISA = qw(AMIDB::Table);

# default constructor (email)
sub new {

    my ($class, $dbh, $email) = @_; 

    my $table_name = "member";
    my $primary_key = { 'email' => $email, };
    # construct the object
    my $self = $class->SUPER::new($dbh, $table_name, $primary_key);

    bless $self, $class;

    return $self;

}

# second constructor (by member_id)
sub new2 {

    my ($class, $dbh, $member_id) = @_;

    my $table_name = "member";
    my $primary_key = { 'member_id' => $member_id, };
    # construct the object
    my $self = $class->SUPER::new($dbh, $table_name, $primary_key);

    bless $self, $class;

    return $self;

}

sub save {

    my ($self) = @_;

    my $q = "select now()";
    my $sth = $self->{'dbh'}->prepare($q);
    $sth->execute;
    ($self->{'columns'}{'date_updated'}) = $sth->fetchrow_array;
    $self->SUPER::save;

}

1;
