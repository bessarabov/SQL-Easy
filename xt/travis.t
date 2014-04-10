=encoding UTF-8
=cut

=head1 DESCRIPTION

This test is run under travic env.

http://docs.travis-ci.com/user/database-setup/#MySQL

=cut

# common modules
use strict;
use warnings FATAL => 'all';
use utf8;

use Test::Most;
use Test::More;
use Test::Deep;
use SQL::Easy;

my $true = 1;
my $false = '';

sub get_se {
    my $se = SQL::Easy->new(
        database => 'sql_easy',
        user     => 'travis',
        password => '',
    );

    return $se;
}

sub create_test_db {

    eval {
        get_se()->execute('DROP TABLE `posts`;');
    };

    get_se()->execute('
CREATE TABLE `posts` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `dt` datetime NOT NULL,
  `title` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
');

    get_se()->insert("
insert INTO `posts` (`id`, `dt`, `title`) values
  ('1', '2010-07-14 18:30:31', 'Hello, World!'),
  ('2', '2010-08-02 17:13:35', 'use perl or die')
");

    return $false;
}

sub test_get_data {
    my $result = get_se()->get_data('select id from posts');

    cmp_deeply(
        $result,
        [
            { id => 1 },
            { id => 2 },
        ],
        'get_data()',
    );

    return $false;
}

sub test_get_one {
    my $result = get_se->get_one('select id from posts where dt > "2010-08-01"');

    cmp_deeply(
        $result,
        2,
        'get_one()',
    );

    return $false;
}

sub test_get_row {
    my @result = get_se->get_row('select id, dt from posts where id = 1');

    cmp_deeply(
        \@result,
        ['1', '2010-07-14 18:30:31'],
        'get_row()',
    );

    return $false;
}

sub test_get_col {
    my @result = get_se->get_col('select id from posts');

    cmp_deeply(
        \@result,
        ['1', '2'],
        'get_col()',
    );

    return $false;
}

sub test_get_tsv_data {
    my $result = get_se->get_tsv_data('select * from posts');

    cmp_deeply(
        $result,
"id\tdt\ttitle
1\t2010-07-14 18:30:31\tHello, World!
2\t2010-08-02 17:13:35\tuse perl or die
",
        'get_tsv_data()',
    );

    return $false;
}

sub main {

    die_on_fail();
    pass('Loaded ok');
    create_test_db();

    test_get_data();
    test_get_one();
    test_get_row();
    test_get_col();
    test_get_tsv_data();

    done_testing();

}
main();
__END__
