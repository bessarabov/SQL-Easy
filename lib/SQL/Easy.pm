package SQL::Easy;

# ABSTRACT: extremely easy access to sql data

=encoding UTF-8
=cut

=head1 DESCRIPTION

On cpan there are a lot of ORMs. The problem is that sometimes ORM are too
complex. You don't need ORM in a simple script with couple requests. ORM is
sometimes difficult to use, you need to learn its syntax. From the other hand
you already knows SQL language.

SQL::Easy give you easy access to data stored in databases using well known
SQL language.

SQL::Easy version numbers uses Semantic Versioning standart.
Please visit L<http://semver.org/> to find out all about this great thing.

=head1 SYNOPSIS

Let image we have db 'blog' with one table:

    CREATE TABLE `posts` (
      `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
      `dt` datetime NOT NULL,
      `title` VARCHAR(255) NOT NULL,
      PRIMARY KEY (`ID`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

    insert INTO `posts` (`dt`, `title`) values
      ('1', '2010-07-14 18:30:31', 'Hello, World!'),
      ('2', '2010-08-02 17:13:35', 'use perl or die')
    ;

Then we we can do some things with SQL::Easy

    use SQL::Easy;

    my $se = SQL::Easy->new( {
        database => 'blog',
        user     => 'user',
        password => 'secret',
        host     => '127.0.0.1',           # default '127.0.0.1'
        port     => 3306,                  # default 3306
        connection_check_threshold => 30,  # default 30
        debug    => 0,                     # default 0
    } );

    # get scalar
    my $posts_count = $se->get_one("select count(id) from posts");

    # get list
    my ($dt, $title) = $se->get_row(
        "select dt, title from posts where id = ?",
        1,
    );

    # get arrayref
    my $posts = $se->get_data(
        "select dt_post, title from posts order by id"
    );
    # We will get
    #    [
    #        {
    #            'dt_post' => '2010-07-14 18:30:31',
    #            'title' => 'Hello, World!'
    #        },
    #        {
    #            'dt_post' => '2010-08-02 17:13:35',
    #            'title' => 'use perl or die'
    #        }
    #    ];

    my $post_id = $se->insert(
        "insert into images ( dt_post, title ) values ( now(), ? )",
        "My new idea"
    );
    # $post_id is the id of the new row in table

    # Sometimes you don't need the any return value (when you delete or update
    # rows), you only need to execute some sql. You can do it by
    $se->execute(
        "update posts set title = ? where id = ?",
        "JAPH",
        2,
    );

If it passed more than 'connection_check_threshold' seconds between requests
the module will check that db connection is alive and reconnect if it went
away.

=cut

use strict;
use warnings;

use DBI;
use Carp;

=head1 METHODS

=cut

=head2 new

B<Get:> 1) $class 2) $params - hashref with connection information

B<Return:> 1) object

    my $se = SQL::Easy->new( {
        database => 'blog',
        user     => 'user',
        password => 'secret',
        host     => '127.0.0.1',           # default '127.0.0.1'
        port     => 3306,                  # default 3306
        connection_check_threshold => 30,  # default 30
        debug    => 0,                     # default 0
    } );

Or, if you already have dbh:

    my $se2 = SQL::Easy->new( {
        dbh => $dbh,
    } );

For example, if you are woring with Dancer::Plugin::Database you can use this
command to create SQL::Easy object:

    my $se3 = SQL::Easy->new( {
        dbh => database(),
    } );

=cut

sub new {
    my ($class, $params) = @_;
    my $self  = {};

    $self->{dbh} = $params->{dbh};
    $self->{connection_check_threshold} = $params->{connection_check_threshold} || 30;
    $self->{debug} = $params->{debug} || 0;
    $self->{count} = 0;

    unless ($self->{dbh}) {
        $self->{settings} = {
            db         => $params->{database},
            user       => $params->{user},
            password   => $params->{password},
            host       => $params->{host} || '127.0.0.1',
            port       => $params->{port} || 3306,
        };

        $self->{dbh} = _get_connection($self->{settings});
    };

    $self->{last_connection_check} = time;

    bless($self, $class);
    return $self;
}

=head2 get_dbh

B<Get:> 1) $self

B<Return:> 1) $ with dbi handler

=cut

sub get_dbh {
    my ($self) = @_;

    $self->_reconnect_if_needed();

    return $self->{dbh};
}

sub return_dbh {
    my ($self) = @_;

    $self->_deprecation_warning("dbh");

    return $self->get_dbh();
}

=head2 get_one

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) $ with the first value of request result

=cut

sub get_one {
    my ($self, $sql, @bind_variables) = @_;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables) or croak $self->{dbh}->errstr;

    my @row = $sth->fetchrow_array;

    return $row[0];
}

sub return_one {
    my ($self, $sql, @bind_variables) = @_;

    $self->_deprecation_warning("one");

    return $self->get_one($sql, @bind_variables);
}

=head2 get_row

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) @ with first row in result table

=cut

sub get_row {
    my ($self, $sql, @bind_variables) = @_;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables) or croak $self->{dbh}->errstr;

    my @row = $sth->fetchrow_array;

    return @row;
}

sub return_row {
    my ($self, $sql, @bind_variables) = @_;

    $self->_deprecation_warning("row");

    return $self->get_row($sql, @bind_variables);
}

=head2 get_col

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) @ with first column in result table

=cut

sub get_col {
    my ($self, $sql, @bind_variables) = @_;
    my @return;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables) or croak $self->{dbh}->errstr;

    while (my @row = $sth->fetchrow_array) {
        push @return, $row[0];
    }

    return @return;
}

sub return_col {
    my ($self, $sql, @bind_variables) = @_;

    $self->_deprecation_warning("col");

    return $self->get_col($sql, @bind_variables);
}

=head2 get_data

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) $ with array of hashes with the result of the query

Sample usage:

    my $a = $se->get_data('select * from t1');

    print scalar @{$a};         # quantity of returned rows
    print $a->[0]{filename};    # element 'filename' in the first row

    for(my $i = 0; $i <= $#{$a}; $i++) {
        print $a->[$i]{filename}, "\n";
    }

=cut

sub get_data {
    my ($self, $sql, @bind_variables) = @_;
    my @return;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables) or croak $self->{dbh}->errstr;

    my @cols = @{$sth->{NAME}};

    my @row;
    my $line_counter = 0;
    my $col_counter = 0;

    while (@row = $sth->fetchrow_array) {
        $col_counter = 0;
        foreach(@cols) {
            $return[$line_counter]{$_} = ($row[$col_counter]);
            $col_counter++;
        }
        $line_counter++;
    }

    return \@return;
}

sub return_data {
    my ($self, $sql, @bind_variables) = @_;

    $self->_deprecation_warning("data");

    return $self->get_data($sql, @bind_variables);
}

=head2 get_tsv_data

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) $ with tab separated db data

Sample usage:

    print $se->get_tsv_data(
        "select dt_post, title from posts order by id limit 2"
    );

It will output the text below (with the tabs as separators).

    dt_post title
    2010-07-14 18:30:31     Hello, World!
    2010-08-02 17:13:35     use perl or die

=cut

sub get_tsv_data {
    my ($self, $sql, @bind_variables) = @_;
    my $return;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables) or croak $self->{dbh}->errstr;

    $return .= join ("\t", @{$sth->{NAME}}) . "\n";

    while (my @row = $sth->fetchrow_array) {
        foreach (@row) {
            $_ = '' unless defined;
        }
        $return .= join ("\t", @row) . "\n";
    }

    return $return;
}

sub return_tsv_data {
    my ($self, $sql, @bind_variables) = @_;

    $self->_deprecation_warning("tsv_data");

    return $self->get_tsv_data($sql, @bind_variables);
}


=head2 insert

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) $ with id of inserted record

Sub executes sql with bind variables and returns id of inseted record

=cut

sub insert {
    my ($self, $sql, @bind_variables) = @_;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables) or croak $self->{dbh}->errstr;

    return $sth->{mysql_insertid};
}

=head2 execute

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> -

Sub just executes sql that it recieves and returns nothing interesting

=cut

sub execute {
    my ($self, $sql, @bind_variables) = @_;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables) or croak $self->{dbh}->errstr;

    return 1;
}

=head2 log_debug

B<Get:> 1) $self 2) $sql

B<Return:> -

If the debug is turned on sub wll print $sql to STDERR

=cut

sub log_debug {
    my ($self, $sql) = @_;

    if ($self->{debug}) {
        $self->{count}++;
        print STDERR "sql " . $self->{count} . ": '$sql'\n";
    }
}

=begin comment _reconnect_if_needed

B<Get:> 1) $self

B<Return:> -

Method checks if last request to db was more than
$self->{connection_check_threshold} seconds ago. If it was, then method
updates stored dbh.

=end comment

=cut

sub _reconnect_if_needed {
    my ($self) = @_;

    if (time - $self->{last_connection_check} > $self->{connection_check_threshold}) {
        if (_check_connection($self->{dbh})) {
            $self->{last_connection_check} = time;
        } else {
            $self->log_debug( "Database connection went away, reconnecting" );
            $self->{dbh}= _get_connection($self->{settings});
        }
    }

}

=begin comment _get_connection

B<Get:> 1) $self

B<Return:> -

Gets hashref with connection parameters and returns db

=end comment

=cut

sub _get_connection {
    my ($self) = @_;

    my $dsn = "DBI:mysql:database=" . $self->{db}
        . ";host=" . $self->{host}
        . ";port=" . $self->{port};

    my $dbh = DBI->connect(
        $dsn,
        $self->{user},
        $self->{password},
        {
            PrintError => 0,
            mysql_auto_reconnect => 0,
            mysql_enable_utf8 => 1,
        },
    ) or croak "Can't connect to database. Error: " . $DBI::errstr . " . Stopped";

    return $dbh;
}

=begin comment _check_connection

B<Get:> 1) $dbh

B<Return:> -

Check the connection is alive.

Based on sub with the same name created by David Precious in
Dancer::Plugin::Database.

=end comment

=cut

sub _check_connection {
    my $dbh = shift;
    return unless $dbh;
    if (my $result = $dbh->ping) {
        if (int($result)) {
            # DB driver itself claims all is OK, trust it:
            return 1;
        } else {
            # It was "0 but true", meaning the default DBI ping implementation
            # Implement our own basic check, by performing a real simple
            # query.
            my $ok;
            eval {
                $ok = $dbh->do('select 1');
            };
            return $ok;
        }
    } else {
        return;
    }
}

sub _deprecation_warning {
    my ($self, $name) = @_;

    croak "Expected 'name'" unless defined $name;

    warn "x"x78 . "\n";
    warn "WARNING. SQL::Easy interface was changed. Since version 0.06 method return_$name() was deprecated. Use get_$name() instead.\n";
    warn "x"x78 . "\n";

}

=head1 SOURCE CODE

The source code for this module is hosted on GitHub
L<https://github.com/bessarabov/SQL-Easy>

=head1 BUGS

Please report any bugs or feature requests in GitHub Issues
L<https://github.com/bessarabov/SQL-Easy>

=cut

1;
