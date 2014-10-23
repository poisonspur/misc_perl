package Table;

use strict;

use Data::Dumper;


#
# This is intended to be an abstract class
# TODO: develop standardization of keys in error hash
#
sub new {

    my ($class, $dbh, $table_name, $primary_key) = @_;

    ($dbh) || (return undef); #database handle required

    # construct the empty object
    my $self = {
        'dbh' => $dbh, # database handle
        'table_name' => $table_name,
        'primary_key' => $primary_key,
        'columns' => {}, # hash reference of db columns 
        'column_info' => {
            'column_names' => [],
            'required' => {},
            'data_type' => {}, 
            'max_length' =>{}, # only for char or varchar date_type
            'auto_increment' =>{}, # if true, field no required on insert
        }, # list of columns and metadata about each
        'sql_statements' => {}, # hashref of prepared statements lazily initialized (this may prove to be retarded) 
        'errors' => {}, # gets populated when bad things happen
        'exists_in_db' => 0, # determines whether to insert or update on save
        'read_only' => 0, # probably overkill but can be set when fields are changed for momentary usefulness
    };

    # need to bless here even though it's supposed to be abstract
    bless $self, $class;

    # check to see if all primary_key fields are populated
    my $pk_ok = 1;
    foreach my $value (values %{$self->{'primary_key'}}) {
        ($value) || ($pk_ok = 0);
    }

    if ($pk_ok) {
        $self->load_by_primary_key;
    }


    return $self;

}

#
# returns a list of the table's columns (fetches if not done so already)
#
sub get_columns {

    my ($self) = @_;

    unless ( @{$self->{'column_info'}{'column_names'}} ) {
        $self->_load_column_info;
    }

    return $self->{'column_info'}{'column_names'};

}

#
# loads the column hash and sets the primary_key and exists_in_db attributes
#
sub load_data {

    my ($self, $href, $exists_in_db) = @_;

    $self->{'columns'} = $href;

    if ($exists_in_db) {
        $self->{'exists_in_db'} = 1;
    }

    foreach my $key ( keys %{$self->{'primary_key'}} ) {
        $self->{'primary_key'}{$key} = $href->{$key};
    }

}

#
# Queries a record from the db on the primary key
#
sub load_by_primary_key {

    my ($self) = @_;

    my $sth = $self->{'table_name'} . "_load_by_primary_key";
    
    #lazy initialization
    unless ($self->{'sql_statements'}{$sth}) {
        my $q = "SELECT * from " . $self->{'table_name'} . " WHERE ";
        foreach my $key ( keys %{$self->{'primary_key'}} ) {
            $q .= " $key = ? AND";
        }
        $q =~ s/ AND$//;
        $self->{'sql_statements'}{$sth} = $self->{'dbh'}->prepare($q);
    }

    my @values = values %{$self->{'primary_key'}};
    $self->{'sql_statements'}{$sth}->execute(@values);
    $self->{'columns'} = $self->{'sql_statements'}{$sth}->fetchrow_hashref;
    if ($self->{'sql_statements'}{$sth}->rows) {
        $self->{'exists_in_db'} = 1;
    }


}

#
# Returns true if the errors hashref is populated, false otherwise
#
sub has_errors {

    my ($self) = @_;

    if ( %{$self->{'errors'}} ) {
        return 1;
    } else {
        return 0;
    }

}

#
# Create a new instance of the object with new data but prepared statements et al. copied over
#
sub clone {

    my ($self, $primary_key) = @_;

    my $new_self = Table->new($self->{'dbh'}, $self->{'table_name'}, $primary_key);
    $new_self->{'sql_statements'} = $self->{'sql_statements'};

    return $new_self;

}

#
#
#
sub save {

    my ($self) = @_;

    if ($self->{'read_only'}) {
        $self->{'errors'}{'read_only'} = "Object is read only.";
    }

    $self->validate_fields;

    unless ( $self->has_errors ) {
        if ($self->{'exists_in_db'}) {
            $self->_update;
        } else {
            $self->_insert;
        }
    }

}

sub delete {

    my ($self) = @_;

    my $sth = $self->{'table_name'} . "_delete";

    unless ($self->{'sql_statements'}{$sth}) {
        my $q = "DELETE FROM " . $self->{'table_name'} . " WHERE ";
        foreach my $key ( keys %{$self->{'primary_key'}} ) {
            $q .= " $key = ? AND";
        }
        $q =~ s/ AND$//;
        $self->{'sql_statements'}{$sth} = $self->{'dbh'}->prepare($q) || die "test";
    }

    my  @values = values %{$self->{'primary_key'}};
    $self->{'sql_statements'}{$sth}->execute(@values);
    $self->{'exists_in_db'} = 0;

}

#
# Checks to see if:
# 1. all required fields are populated
# 2. numeric and date fields are valid
# 3. String fields do not exceed the maximum length
#
sub validate_fields {

    my ($self) = @_;

    unless ( @{$self->{'column_info'}{'column_names'}} ) {
        $self->_load_column_info;
    }

    foreach my $column ( @{$self->{'column_info'}{'column_names'}} ) {
        
        # for paranoia's sake, remove trailing whitespace
        # self->{'columns'}{$column} =~ s/\s+$//;

        # Check if required field is empty
        if ($self->{'column_info'}{'required'}{$column}) {
            if ( (! defined $self->{'columns'}{$column}) || (! $self->{'columns'}{$column}) ) {
                if ($self->{'exists_in_db'}) { 
                    $self->{'errors'}{"$column required"} = "$column: Field is required.";
                } else { # on inserts, let auto_increment fields slide
                    unless ( $self->{'column_info'}{'auto_increment'}{$column} ) {
                        $self->{'errors'}{"$column required"} = "$column: Field is required.";
                    }
                }
            } 
        }

        # Check date fields (TODO: datetime and timestamp)
        if ( $self->{'column_info'}{'data_type'}{$column} eq 'date') {
            if ( $self->{'columns'}{$column} && $self->{'columns'}{$column} !~ /^\d\d\d\d\-\d\d\-\d\d$/) {
                $self->{'errors'}{"$column format"} = "$column: Format is yyyy-mm-dd";
            }
        }

        # Check integer fields (TODO: float and decimal fields)
        if ($self->{'column_info'}{'data_type'}{$column} eq 'int' ||
            $self->{'column_info'}{'data_type'}{$column} eq 'tinyint' ||
            $self->{'column_info'}{'data_type'}{$column} eq 'smallint' ||
            $self->{'column_info'}{'data_type'}{$column} eq 'bigint') {
            if ( $self->{'columns'}{$column} && $self->{'columns'}{$column} !~ /^\d+$/) {
                $self->{'errors'}{"$column integer"} = "$column: Must be an integer";
            }
        }

        # Check to see if maximum length is exceeded for char and varchar fields
        if ($self->{'column_info'}{'data_type'}{$column} eq 'char' ||
            $self->{'column_info'}{'data_type'}{$column} eq 'varchar') {
            if ( defined $self->{'columns'}{$column}) {
                if ( (length $self->{'columns'}{$column}) > $self->{'column_info'}{'max_length'}{$column} ) {
                    $self->{'errors'}{"$column max_length"} = "$column: Exceeds maximum length of $self->{'column_info'}{'max_length'}{$column}";
                }
            }
        }


    }

}

#############################################################
# Methods intended for internal use only
#############################################################

sub _insert {

    my ($self) = @_;

    my $sth = $self->{'table_name'} . "_insert";
    my $dbh = $self->{'dbh'};

    #lazy initialization
    my @values = ();
    unless ($self->{'sql_statements'}{$sth}) {
        my $q = "INSERT INTO " . $self->{'table_name'} . " (";
        foreach my $column (keys %{$self->{'columns'}}) {
            unless ( $self->{'column_info'}{'auto_increment'}{$column} ) {
                $q .= "$column,";
            }
        }
        $q =~ s/,$//;
        $q .= ") VALUES (";
        foreach my $column (keys %{$self->{'columns'}}) {
            unless ( $self->{'column_info'}{'auto_increment'}{$column} ) {
                $q .= "?,";
            }
        }
        $q =~ s/,$//;
        $q .= ")";
        $self->{'sql_statements'}{$sth} = $self->{'dbh'}->prepare($q) || die "test";

	### print "<H1>Query: <TT>$q</TT></H1>"; ###DEBUG

        foreach my $column (keys %{$self->{'columns'}}) {
            unless ( $self->{'column_info'}{'auto_increment'}{$column} ) {
                push @values, $self->{'columns'}{$column};
            }
        }
    }

    ### my $vvv = join ",", @values;
    ### print "<H1>Values: <TT><BR>($vvv)</TT></H1>"; ###DEBUG

    $self->{'sql_statements'}{$sth}->execute(@values) || die $dbh::errstr;
    foreach my $column (keys %{$self->{'column_info'}{'auto_increment'}}) {
        if ($self->{'column_info'}{'auto_increment'}{$column}) {
            # my $q = "SELECT MAX($column) FROM " . $self->{'table_name'};
            my $q = "select last_insert_id()";
            my $sth = $self->{'dbh'}->prepare($q);
            $sth->execute;
            ($self->{'columns'}{$column}) = $sth->fetchrow_array;
        }
    }

}

sub _update {

    my ($self) = @_;

    my $sth = $self->{'table_name'} . "_update";

    #lazy initialization
    unless ($self->{'sql_statements'}{$sth}) {
        my $q = "UPDATE " . $self->{'table_name'} . " SET ";
        foreach my $column (keys %{$self->{'columns'}}) {
            $q .= "$column = ?,";
        }
        $q =~ s/,$//;
        $q .= " WHERE ";
        foreach my $key ( keys %{$self->{'primary_key'}} ) {
            $q .= " $key = ? AND";
        }
        $q =~ s/ AND$//;
        $self->{'sql_statements'}{$sth} = $self->{'dbh'}->prepare($q) || die "test";
    }

    my @values = values %{$self->{'columns'}};
    push @values, values %{$self->{'primary_key'}};
    $self->{'sql_statements'}{$sth}->execute(@values) || die $dbh::errstr;;

}

#
# Populates metadata fields and column list
#
sub _load_column_info {

    my ($self) = @_;

    my $columns_sth = $self->{'dbh'}->prepare("describe $self->{'table_name'}");
    $columns_sth->execute;
    while (my $href = $columns_sth->fetchrow_hashref) {
        push @{$self->{'column_info'}{'column_names'}}, $href->{'Field'};
        if ($href->{'Null'} eq 'YES') {
            $self->{'column_info'}{'required'}{$href->{'Field'}} = 0;
        } else {
            $self->{'column_info'}{'required'}{$href->{'Field'}} = 1;
        }
        $href->{'Type'} =~ /(^\w+).*?(\d+|$)/;
        my $data_type = $1;
        my $max_length = $2;
        $self->{'column_info'}{'data_type'}{$href->{'Field'}} = $data_type;
        $self->{'column_info'}{'max_length'}{$href->{'Field'}} = $max_length;
        if ($href->{'Extra'}  =~ /auto_increment/) {
            $self->{'column_info'}{'auto_increment'}{$href->{'Field'}} = 1;
        } else {
            $self->{'column_info'}{'auto_increment'}{$href->{'Field'}} = 0;
        }
    }


}
return 1;
