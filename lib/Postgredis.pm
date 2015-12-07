package Postgredis;

use Mojo::Pg;
use v5.20;
use experimental 'signatures';
use strict;

our $VERSION=0.01;

sub new {
    my $s = shift;
    my @a = @_;
    my %args;
    %args = ( namespace => $_[0] ) if @_==1;
    bless \%args, $s;
}

sub namespace($s,$new=undef) {
    $s->{namespace} = $new if @_==2;
    $s->{namespace};
}

sub _pg($s) {
    state $db;
    return $db if defined($db);
    $db = Mojo::Pg->new;
    $ENV{PG_CONNECT_STR} and do { $db = $db->from_string( $ENV{PG_CONNECT_STR} ) };
    $ENV{PG_CONNECT_DSN} and do { $db = $db->dsn($ENV{PG_CONNECT_DSN}) };
    $db;
}

sub pg($s) { $s->_pg->db; }

sub _create_tables($s) {
    my $table = $s->namespace;
    $s->query(<<DONE);
    create table $table (
        k varchar not null primary key,
        v jsonb
    )
DONE
    $s->query(<<DONE);
    create table $table\_sorted (
        k varchar not null,
        v jsonb not null,
        score real not null,
    primary key (k, v)
    )
DONE
    $s->query(<<DONE);
    create index on $table\_sorted (k,score)
DONE
}

sub _drop_tables($s) {
    my $table = $s->namespace;
    $s->query("drop table if exists $table");
    $s->query("drop table if exists $table\_sorted");
}

sub _tables_exist($s) {
    my $res = $s->query(q[select 1 from information_schema.tables where table_name = ?],
		$s->namespace);
	return $res->rows > 0;
}

sub query($s,$str, @more) {
    my $namespace = $s->namespace;
    $str =~ s/\bredis\b/$namespace/;
    $str =~ s/\bredis_sorted\b/$namespace\_sorted/;
    return $s->pg->query($str, @more);
}

sub maybe_init($s) {
	$s->flushdb unless $s->_tables_exist;
    $s;
}

sub flushdb($s) {
    $s->_drop_tables if $s->_tables_exist;
    $s->_create_tables;
    return $s;
}

sub default_ttl { }

sub set($s,$key,$value) {
  my $res;
  $res = $s->query("update redis set v = ?::jsonb where k = ?", { json => $value }, $key);
  return 1 if $res->rows > 0;
  $s->query("insert into redis (k, v) values (?,?::jsonb)", $key, { json => $value } );
  return 1;
}

sub get($s,$k) {
    return $s->query("select v from redis where k=?",$k)->expand->array->[0];
}

sub del($s,$k) {
    $s->query("delete from redis where k=?",$k);
}

sub keys($s,$pat) {
    $pat =~ s/\*/%/g;
    return $s->query("select k from redis where k like ?",$pat)->arrays->flatten;
}

sub exists($s,$k) {
    my $got = $s->query("select * from redis where k=?",$k);
    return $got->rows > 0;
}

sub hset($s,$key,$hkey,$value) {
    my $res = $s->query("select v from redis where k = ?", $key)->expand;
    my $json = $res->rows ? $res->hash->{v} : {};
    $json->{$hkey} = $value;
    $res = $s->query("update redis set v = ?::jsonb where k = ?",{json=>$json},$key);
    return 1 if $res->rows > 0;
    $res = $s->query("insert into redis (k, v) values (?,?::jsonb)",$key, {json=>$json});
    return 1;
}

sub hdel($s,$key,$hkey) {
    my $json = $s->query("select v from redis where k = ?", $key)->expand->hash->{v};
    exists($json->{$hkey}) or return 0;
    delete $json->{$hkey} or return 0;
    $s->query("update redis set v = ?::jsonb where k = ?",{json=>$json},$key);
}

sub hget($s,$key,$hkey) {
    my $json = $s->query("select v from redis where k = ?", $key)->expand->hash->{v};
    return $json->{$hkey};
}

sub hgetall($s,$key) {
    my $res = $s->query("select v from redis where k = ?", $key)->expand;
    return {} unless $res->rows;
    return $res->hash->{v};
}

sub sadd($s,$key,$value) {
    $s->hset($key,$value,1);
}

sub srem($s,$key,$value) {
    my $json = $s->query("select v from redis where k = ?", $key)->expand->hashes;
    $json &&= $json->[0]{v};
    delete $json->{$value};
    $s->query("update redis set v = ?::jsonb where k = ?",{json=>$json},$key);
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

sub zadd($s,$key,$val,$score) {
    $s->query("insert into redis_sorted (k,score,v) values (?,?,?::jsonb)",
        $key, $score,{ json => $val });
}

sub zscore($s,$key,$val) {
    return $s->query("select score from redis_sorted where k = ? and v = ?::jsonb",
        $key, { json => $val })->array->[0];
}

sub zrem($s,$key,$val) {
    $s->query("delete from redis_sorted where k = ? and v = ?::jsonb", $key, { json => $val } );
}

sub zrangebyscore($s,$key,$min,$max) {
    return $s->query("select v from redis_sorted where k = ? and score >= ?
        and score <= ? order by score, v::text", $key, $min, $max)
    ->expand->arrays->flatten;
}

1;

