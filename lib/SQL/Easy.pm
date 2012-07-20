package SQL::Easy;

=encoding UTF-8
=cut

=head1 NAME

SQL::Easy - extremely easy access to sql data

=head1 DESCRIPTION

On cpan there are a lot of ORMs. The problem is that sometimes ORM are too
complex. You don't need ORM in a simple script with couple requests. ORM is
sometimes difficult to use, you need to learn its syntax. From the other hand
you already knows SQL language.

SQL::Easy give you easy access to data stored in databases using well known
SQL language.

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
    my $posts_count = $se->return_one("select count(id) from posts");

    # get list
    my ($dt, $title) = $se->return_row(
        "select dt, title from posts where id = ?",
        1,
    );

    # get arrayref
    my $posts = $se->return_data(
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

=head1 AUTHOR

Ivan Bessarabov, C<< <ivan@bessarabov.ru> >>

=head1 SOURCE CODE

The source code for this module is hosted on GitHub
L<https://github.com/bessarabov/SQL-Easy>

=head1 BUGS

Please report any bugs or feature requests in GitHub Issues
L<https://github.com/bessarabov/SQL-Easy>

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Ivan Bessarabov.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

use strict;
use warnings;

our $VERSION = 0.04;

use DBI;

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

=head2 return_dbh

B<Get:> 1) $self

B<Return:> 1) $ with dbi handler

=cut

sub return_dbh {
    my ($self) = @_;

    $self->_reconnect_if_needed();

    return $self->{dbh};
}

=head2 return_one

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) $ with the first value of request result

=cut

sub return_one {
    my ($self, $sql, @bind_variables) = @_;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables);

    my @row = $sth->fetchrow_array;

    return $row[0];
}

=head2 return_row

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) @ with first row in result table

=cut

sub return_row {
    my ($self, $sql, @bind_variables) = @_;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables);

    my @row = $sth->fetchrow_array;

    return @row;
}

=head2 return_col

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) @ with first column in result table

=cut

sub return_col {
    my ($self, $sql, @bind_variables) = @_;
    my @return;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables);

    while (my @row = $sth->fetchrow_array) {
        push @return, $row[0];
    }

    return @return;
}

=head2 return_data

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) $ with array of hashes with the result of the query

Sample usage:

    my $a = $se->return_data('select * from t1');

    print scalar @{$a};         # quantity of returned rows
    print $a->[0]{filename};    # element 'filename' in the first row

    for(my $i = 0; $i <= $#{$a}; $i++) {
        print $a->[$i]{filename}, "\n";
    }

=cut

sub return_data {
    my ($self, $sql, @bind_variables) = @_;
    my @return;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables);

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

=head2 return_tsv_data

B<Get:> 1) $self 2) $sql 3) @bind_variables

B<Return:> 1) $ with tab separated db data

Sample usage:

    print $se->return_tsv_data(
        "select dt_post, title from posts order by id limit 2"
    );

It will output the text below (with the tabs as separators).

    dt_post title
    2010-07-14 18:30:31     Hello, World!
    2010-08-02 17:13:35     use perl or die

=cut

sub return_tsv_data {
    my ($self, $sql, @bind_variables) = @_;
    my $return;

    $self->_reconnect_if_needed();

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@bind_variables);

    $return .= join ("\t", @{$sth->{NAME}}) . "\n";

    while (my @row = $sth->fetchrow_array) {
        foreach (@row) {
            $_ = '' unless defined;
        }
        $return .= join ("\t", @row) . "\n";
    }

    return $return;
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
    $sth->execute(@bind_variables);

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
    $sth->execute(@bind_variables);

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
            RaiseError => 1,
            PrintError => 0,
            mysql_auto_reconnect => 0,
            mysql_enable_utf8 => 1,
        },
    );

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

1;
