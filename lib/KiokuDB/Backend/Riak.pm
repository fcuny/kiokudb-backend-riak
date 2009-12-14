package KiokuDB::Backend::Riak;

use Carp;
use Moose;
use JSON::XS;
use AnyEvent::Riak;
use Try::Tiny;

use namespace::clean -except => 'meta';
use Data::Stream::Bulk::Util qw(bulk);

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Serialize::JSPON
);

our $VERSION = '0.01';

has bucket => (isa => 'Str', is => 'rw');

has db => (
    isa => "AnyEvent::Riak",
    is => "ro",
    handles => [qw(document)],
);

sub new_from_dsn_params {
    my ( $self, %args ) = @_;

    my $host = delete $args{host};
    my $path = delete $args{path};

    $self->bucket( delete $args{bucket} );

    croak "bucket is required" if !$self->bucket;
    my $db = AnyEvent::Riak->new( { 'host' => $host, path => $path } );
    $self->new( %args, db => $db );
}

sub get {
    my ( $self, @ids ) = @_;
    return map {$self->get_entry($_)} @ids;
}

sub get_entry {
    my ($self, $id) = @_;
    my $entry;
    try {
        my $obj = $self->db->fetch($self->bucket, $id)->recv;
        $entry = $self->deserialize($obj->{object});
    };
    return $entry;
}

sub deserialize {
    my ( $self, $doc ) = @_;
    my %doc = %{ $doc };
    return $self->expand_jspon(\%doc, backend_data => $doc );
}

sub exists {
    my ( $self, @ids ) = @_;

    my @exists;

    foreach my $id (@ids) {
        my $res;
        try {
            $res = $self->db->fetch($self->bucket, $_)->recv;
            push @exists, 1
        }catch{
            push @exists, 0;
        };
    }
    return @exists;
}

sub delete {
    my ($self, @ids_or_entries) = @_;
    my @ids = map { ref($_) ? $_->id : $_ } @ids_or_entries;
    $self->db->delete($self->bucket, $_)->recv foreach (@ids);
}

sub clear {
    my ($self, ) = @_;
    try {
        my $res = $self->db->list_bucket($self->bucket);
        return $self->delete(@{$res->{keys}});
    };
}

sub all_entries {
    my ($self) = @_;
    try {
        my $res = $self->db->list_bucket($self->bucket);
        return $self->get(@{$res->{keys}});
    };
}

sub insert {
    my ($self, @entries) = @_;
    for my $entry (@entries) {
        $self->insert_entry($entry);
    }
    return;
}

sub insert_entry {
    my ( $self, $entry ) = @_;
    my $check;
    try {
        my $res = $self->db->store(
            {
                bucket => $self->bucket,
                key    => $entry->id,
                object => $self->collapse_jspon($entry),
                links  => []
            }
        )->recv;
    };
}

1;

__END__

=head1 NAME

KiokuDB::Backend::Riak - Riak backend for L<KiokuDB>

=head1 SYNOPSIS

  use KiokuDB::Backend::Riak;

=head1 DESCRIPTION

This backend provides L<KiokuDB> support for Riak using <AnyEvent::Riak>.

=head1 AUTHOR

franck cuny E<lt>franck@lumberjaph.netE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright 2009 by linkfluence.

L<http://linkfluence.net>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
