use strict;
use warnings;
use Test::More;

use Test::Pod::Coverage 1.08;

all_pod_coverage_ok(
    { also_private => [ qr/^return_/ ], },
);
