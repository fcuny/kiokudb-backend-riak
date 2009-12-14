#!/usr/bin/perl

use Scope::Guard;
use Test::More 'no_plan';

#BEGIN {
#    plan skip_all => 'Please set KIOKU_RIAK_URI to a Riak instance URI' unless $ENV{KIOKU_RIAK_URI};
#    plan 'no_plan';
#}

use ok 'KiokuDB';
use ok 'KiokuDB::Backend::Riak';

use KiokuDB::Test;

use AnyEvent::Riak;

my $db = AnyEvent::Riak->new(
    host => 'http://localhost:8098',
    path => 'jiak',
);

my $bucket = $db->list_bucket('kiokudb')->recv;
foreach my $key (@{$bucket->{keys}}) {
    $db->delete('kiokudb', $key)->recv;
}

run_all_fixtures(
    KiokuDB->new(
        backend => KiokuDB::Backend::Riak->new(
            db => $db,
            bucket => 'kiokudb',
        ),
    )
);
