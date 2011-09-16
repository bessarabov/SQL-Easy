use inc::Module::Install;

name            'SQL-Easy';
author          'Ivan Bessarabov <ivan@bessarabov.ru>';
version         '0.04';
license         'perl';

requires        'DBI';

auto_install;
WriteAll;
