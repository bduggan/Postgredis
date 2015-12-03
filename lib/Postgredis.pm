package Postgredis;

use Mojo::Pg;
use Mojo::Base qw/-base/;
use experimental 'signatures';
use strict;

our $VERSION=0.01;

# Notes:
# comparison to redis
#       1. +no need to manually maintain indexes.
#       2. +native JSON (jsonb) type
#       3. -parallel scaling across machines harder
#
$ENV{PG_CONNECT} //= 'postgresql:///default';
has _pg => sub { state $db //= Mojo::Pg->new( $ENV{PG_CONNECT} // die "set PG_CONNECT") };
has pg => sub { shift->_pg->db; };
has namespace => sub { die "no namespace" };

sub new {
    my $s = shift;
    my @a = @_;
    my %args;
    %args = ( namespace => $_[0] ) if @_==1;
    my $t = $s->SUPER::new(%args);
    $t;
}

sub migrations($s) {
    my $table = $s->namespace;
    return $s->_pg->migrations->from_string(<<DONE);
-- 1 up
create table $table (
    key varchar not null primary key,
    value varchar,
    jv jsonb
);
-- 1 down
drop table if exists $table;
DONE
}

sub query($s,$str, @more) {
    my $namespace = $s->namespace;
    $str =~ s/\bredis\b/$namespace/;
    return $s->pg->query($str, @more);
}

sub maybe_init($s) {
    my $res = $s->query(q[select 1 from information_schema.tables where table_name = ?],
		$s->namespace);
	return $s if $res->rows > 0;
	$s->flushdb;
}

sub flushdb($s) {
    $s->migrations->migrate(0);
    $s->migrations->migrate;
    return $s;
}

sub default_ttl { }

sub set($s,$key,$value) {
  my $res;
  my $column = $value =~ /^\{.*\}$/ ? 'jv' : 'value';
  $res = $s->query("update redis set $column = ? where key = ?", $value, $key);
  return 1 if $res->rows > 0;
  $s->query("insert into redis (key, $column) values (?,?)", $key, $value);
  return 1;
}

sub get($s,$k) {
    return $s->query("select value from redis where key=?",$k)->array->[0];
}

sub del($s,$k) {
    $s->query("delete from redis where key=?",$k);
}

sub keys($s,$pat) {
    $pat =~ s/\*/%/g;
    return $s->query("select key from redis where key like ?",$pat)->arrays->flatten;
}

sub exists($s,$k) {
    my $got = $s->query("select * from redis where key=?",$k);
    return $got->rows > 0;
}

sub hset($s,$key,$hkey,$value) {
    my $res = $s->query("select jv from redis where key = ?", $key)->expand;
    my $json = $res->rows ? $res->hash->{jv} : {};
    $json->{$hkey} = $value;
    $res = $s->query("update redis set jv = ?::jsonb where key = ?",{json=>$json},$key);
    return 1 if $res->rows > 0;
    $res = $s->query("insert into redis (key, jv) values (?,?::jsonb)",$key, {json=>$json});
    return 1;
}

sub hdel($s,$key,$hkey) {
    my $json = $s->query("select jv from redis where key = ?", $key)->expand->hash->{jv};
    exists($json->{$hkey}) or return 0;
    delete $json->{$hkey} or return 0;
    $s->query("update redis set jv = ?::jsonb where key = ?",{json=>$json},$key);
}

sub hget($s,$key,$hkey) {
    my $json = $s->query("select jv from redis where key = ?", $key)->expand->hash->{jv};
    return $json->{$hkey};
}

sub hgetall($s,$key) {
    my $res = $s->query("select jv from redis where key = ?", $key)->expand;
    return {} unless $res->rows;
    return $res->hash->{jv};
}

sub sadd($s,$key,$value) {
    $s->hset($key,$value,1);
}

sub srem($s,$key,$value) {
    my $json = $s->query("select jv from redis where key = ?", $key)->expand->hashes;
    $json &&= $json->[0]{jv};
    delete $json->{$value};
    $s->query("update redis set jv = ?::jsonb where key = ?",{json=>$json},$key);
    return 1;
}

sub smembers($s,$k) {
    my $j = $s->hgetall($k);
    return [ CORE::keys(%$j) ]
}

sub incr($s,$k) {
    my $exists = $s->query("select 1 from pg_class where relname = ?", $k);
    $k =~ /^[a-z0-9:_]+$/ or die "bad sequence name $k";
    unless ($exists->rows) {
        $s->query("create sequence $k start 1");
    }
    my $next = $s->query("select nextval(?)",$k)->arrays->flatten;
    return $next->[0];
}

sub zadd($s,$key,$k,$score) {
    # todo: make an index using jsonb + gin
    $s->hset($key,$k,$score);
}

sub zscore($s,$key,$k) {
    $s->hget($key,$k);
}

sub zrem($s,$key,$k) {
    $s->hdel($key, $k)
}

sub zrangebyscore($s,$key,$min,$max) {
    # TODO optimize and use index and avoid sort
    my $j = $s->hgetall($key);
    my @sorted = sort { $j->{$a} <=> $j->{$b} } CORE::keys %$j;
    my @ok = grep { $j->{$_} >= $min && $j->{$_} <= $max } @sorted;
    return \@ok;
}

1;

