package SQL::Easy;

=encoding UTF-8
=cut

=head1 NAME

SQL::Easy - extremely easy access to sql data

=head1 DESCRIPTION

On cpan there are a lot of ORMs. The problem is that sometimes ORM are too complex.
You don't need ORM in a simple script with couple requests. ORM is sometimes difficult to use,
you need to learn its syntax. From the other hand you already knows SQL language.

SQL::Easy give you easy access to data stored in databases using well known SQL language.

=head1 SYNOPSIS

    use SQL::Easy;
    use Data::Dumper; # this is only for example, you don't need it in real script
    
    # create object
    my $se = SQL::Easy->new( {
            database => 'my_blog',
            user => 'user',
            password => 'secret',
            host => 'localhost',
            port => '3306',
            debug => 0,
    } );

    # or, if you already have dbh:
    # ( example how to you can use SQL::Easy with Dancer::Plugin::Database )
    my $dbh = database();
    my $se2 = SQL::Easy->new( { dbh=>$dbh } );

    # let's find out how many blog posts do I have:
    my $posts_count = $se->return_one('select count(id) from posts');
    print Dumper $posts_count; # will print $VAR1 = 42;

    # some data about post with id 1
    my @a = $se->return_row("select dt_post, title from posts where id = ?", 1);
    print Dumper @a;
    # will print:
    =head2
        $VAR1 = '2010-07-14 18:30:31';
        $VAR2 = 'Hello, World!';
    =cut

    # some data about 2 posts:
    print Dumper $se->return_data("select dt_post, title from posts order by id limit 2");
    =head2
        $VAR1 = [
                  {
                    'dt_post' => '2010-07-14 18:30:31',
                    'title' => 'Hello, World!'
                  },
                  {
                    'dt_post' => '2010-08-02 17:13:35',
                    'title' => 'use perl or die'
                  }
                ];
    =cut

    # Next. Let add new post:
    print Dumper $se->insert("insert into images ( dt_post, title ) values ( now(), ? )", "My new idea");
    # It will print
    # $VAR1 = 43;
    # and 43 is the id of the new row in table
 
    # Sometimes you don't need the any return value (when you delete or update rows),
    # you only need to execute some sql. You can do it by
    $se->execute("update posts set title = ? where id = ?", "JFDI", 2);

=cut

use strict;
use warnings;

our $VERSION = 0.01;

use DBI;

=head1 GENERAL FUNCTIONS
=cut

=head2 new
 
 * Get: 1) hash with connection information 
 * Return: 1) object 

Sub creates an object

=cut

sub new {
    my $class = shift;
    my $self  = {};
    my ($params) = @_;
    
    $self->{dbh} = $params->{dbh};
    $self->{debug} = $params->{debug};
    $self->{count} = 0;

    unless ($self->{dbh}) {
        my $host       = $params->{host} || 'localhost';
        my $port       = $params->{port} || 3306;
        my $db         = $params->{database};
        my $user       = $params->{user};
        my $password   = $params->{password};

        my $dsn = "DBI:mysql:database=$db;host=$host;port=$port;mysql_enable_utf8=1";
        $self->{dbh} = DBI->connect($dsn, $user, $password, { RaiseError => 1,
                PrintError => 0
            });
    };

    bless($self, $class);
    return $self;
}

=head2 return_dbh
 
 * Get: - 
 * Return: 1) $ with dbi handler 

=cut

sub return_dbh {
    my ($self) = @_;
    
    return $self->{dbh};
}

=head2 return_one
 
 * Get: 1) $ sql 2) @ bind variables
 * Return: 1) $ with first value of request result

=cut

sub return_one {
    my ($self, $sql, @a) = @_;

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@a);

    my @row = $sth->fetchrow_array;
    
    return $row[0];
}

=head2 return_row
 
 * Get: 1) $ sql 2) @ bind variables
 * Return: 1) @ with first row in result table

=cut

sub return_row {
    my ($self, $sql, @a) = @_;

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@a);

    my @row = $sth->fetchrow_array;
    
    return @row;
}

=head2 return_col
 
 * Get: 1) $ sql 2) @ bind variables
 * Return: 1) @ with first column in result table

=cut

sub return_col {
    my ($self, $sql, @a) = @_;

    my @return;

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@a);

    while (my @row = $sth->fetchrow_array) {
        push @return, $row[0]; 
    }

    return @return;
}

=head2 return_data
 
 * Get: 1) $ sql 2) @ bind variables
 * Return: 1) $ with array of hashes with the result of the query

Sample usage:

    my $a = $se->return_data('select * from t1');

    print scalar @{$a};         # quantity of returned rows
    print $a->[0]{filename};    # element 'filename' in the first row

    for(my $i = 0; $i <= $#{$a}; $i++) {
        print $a->[$i]{filename}, "\n";
    }

=cut

sub return_data {
    my $self = shift;
    my ($sql, @a) = @_;
    my @return;

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@a);

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

=head2 insert
 
 * Get: 1) $ sql 2) @ bind variables
 * Return: 1) $ with id of inserted record

Sub executes sql with bind variables and returns id of inseted record

=cut

sub insert {
    my $self = shift;
    my ($sql, @a) = @_;
    
    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@a);

    return $sth->{mysql_insertid};
}

=head2 execute
 
 * Get: 1) $ sql 2) @ bind variables
 * Return: -

Sub just executes sql that it recieves and returns noting interesting

=cut

sub execute {
    my $self = shift;
    my ($sql, @a) = @_;

    my $sth = $self->{dbh}->prepare($sql);
    $self->log_debug($sql);
    $sth->execute(@a);

    return 1;
}

=head2 log_debug 
 
 * Get: 1) $ with debug text 
 * Return: -

If the debug is turned on sub prints sql queries that are executed

=cut

sub log_debug {
    my $self = shift;
    my ($sql) = @_;

    if ($self->{debug}) {
        $self->{count}++;
        print STDERR "sql " . $self->{count} . ": '$sql'\n";
    }
}

=head1 AUTHOR

Ivan Bessarabov, C<< <ivan@bessarabov.ru> >>

=head2 SOURCE CODE 

The source code for this module is hosted on GitHub http://github.com/bessarabov/SQL-Easy

=cut

1;
   
