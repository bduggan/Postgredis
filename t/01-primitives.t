#!perl

use Test::More;
use Postgredis;
use Test::PostgreSQL;

my $psql = Test::PostgreSQL->new() or plan
    skip_all => $test::postgresql::errstr;

$ENV{PG_CONNECT_STR} = "postgresql:///test";
$ENV{PG_CONNECT_DSN} = $psql->dsn;

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

# Hashes
ok $db->hset("good","night","moon");
is $db->hget("good","night"), "moon";
is_deeply $db->hgetall("good"),{ night => "moon" };
ok $db->hdel("good","night");

# Sets
for my $i (1..5) {
    ok $db->sadd('nums',$i);
}
is_deeply [ sort @{ $db->smembers('nums')} ], [1..5];
ok $db->srem("nums",3);
is_deeply [ sort @{ $db->smembers('nums')} ], [1,2,4,5];

# Sorted sets
ok $db->zadd(letters => ('c', 10));
ok $db->zadd(letters => ('d', 5));
ok $db->zadd(letters => ('a', 1));
is $db->zscore(letters => 'a'), 1;
is $db->zscore(letters => 'c'), 10;
is $db->zscore(letters => 'd'), 5;
is_deeply $db->zrangebyscore('letters', 2, 20), ['d','c'];
ok $db->zrem(letters => 'a');
ok $db->zrem(letters => 'c');

# Counters
my $start = $db->incr("countme");
is $db->incr("countme"), $start + 1;
is $db->incr("countme"), $start + 2;
my $nother = $db->incr("countme2");
is $db->incr("countme2"), $nother + 1;

done_testing();

