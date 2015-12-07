#!perl

use Test::More;
use Postgredis;
use Test::PostgreSQL;

my $psql;
if ($ENV{TEST_PG_CONNECT_STR}) {
    $ENV{PG_CONNECT_STR} = $ENV{TEST_PG_CONNECT_STR};
} else {
    $psql = Test::PostgreSQL->new() or plan
        skip_all => $test::postgresql::errstr;
    $ENV{PG_CONNECT_STR} = "postgresql:///test";
    $ENV{PG_CONNECT_DSN} = $psql->dsn;
}

my $db = Postgredis->new('test_namespace')->flushdb;

# Keys
ok $db->set("hi","there");
ok $db->set("hi","there");
is $db->get("hi"), "there";
ok $db->set("hi","here");
is $db->get("hi"), "here";
ok $db->set("hi:2","here");
ok $db->set("hi:3","here");
is_deeply $db->keys('hi:*'), [ 'hi:2', 'hi:3' ];
ok $db->exists("hi");
ok !$db->exists("hi9");
ok $db->del("hi:3");
ok !$db->exists("hi:3");

# JSON values
ok $db->set("hello", { world => 42 });
is_deeply($db->get("hello"), { world => 42 } );

# More values
for my $str (
    q[don't],
    q[xx"zz],
    q[zz\\z],
    q[rêsumé],
    q[♠	♡ ♢ ♣],
) {
    ok $db->set(val => $str);
    is $db->get(val), $str;
}

# Hashes
ok $db->hset("good",night => "moon"), "hset";
ok $db->hset("bad",moon => "rising"), "hset";
is $db->hget("good","night"), "moon", "hget";
is $db->hget("bad","moon"), "rising", "hget";
is_deeply $db->hgetall("good"),{ night => "moon" }, "hgetall";
ok $db->hdel("good","night"), "hdel";

# Sets
for my $i (5,4,2,1,3) {
    ok $db->sadd('nums',$i), "sadd";
}
is_deeply [ sort @{ $db->smembers('nums')} ], [1..5], "smembers";
ok $db->srem("nums",3), "srem";
is_deeply [ sort @{ $db->smembers('nums')} ], [1,2,4,5], "smembers";

# Sorted sets
ok $db->zadd(letters => ('c', 10)), 'zadd';
ok $db->zadd(letters => ('d', 5)), 'zadd';
ok $db->zadd(letters => ('a', 1)), 'zadd';
is $db->zscore(letters => 'a'), 1, 'zscore';
is $db->zscore(letters => 'c'), 10, 'zscore';
is $db->zscore(letters => 'd'), 5, 'zscore';
is_deeply $db->zrangebyscore('letters', 2, 20), ['d','c'], 'zrangebyscore';
ok $db->zrem(letters => 'a'), "zrem";
ok $db->zrem(letters => 'c'), "zrem";

# Counters
my $start = $db->incr("countme");
is $db->incr("countme"), $start + 1, "incr";
is $db->incr("countme"), $start + 2, "incr";
my $nother = $db->incr("countme2");
is $db->incr("countme2"), $nother + 1, "incr";

done_testing();

