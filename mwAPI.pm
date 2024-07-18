package mwAPI;
use     strict;
use     HTTP::Request;
use     HTTP::Request::Common;
use     LWP::UserAgent;
use     XML::Simple;
use     URI::Escape;
use     Data::Dumper;
use     version; our $VERSION = version->declare("v1.0.3");                                                           

sub new {
  shift;
  my $self = {};
  bless $self;
  $self->{ua} = new LWP::UserAgent;
  $self->{ua}->default_headers->push_header('Accept-Encoding' => 'gzip');
  $self->{ua}->cookie_jar({});
  return $self;
}

sub ua { $_[0]->{ua}; }

sub login {
  my $self = shift;
  my( $username, $password ) = @_;
  $self->{username} ||= $username;
  $self->{password} ||= $password;
  my $data = $self->api2data( 'login', [lgname => $self->{username}, lgpassword => $self->{password}] );
  my $token = $data->{login}{token};
  my $data = $self->api2data( 'login', [lgname => $self->{username}, lgpassword => $self->{password}, lgtoken => $token] );
  my $self->{'logged_in'} = ($data->{login}{result} eq 'Success');
}

sub check_login {
  my $self = shift;;  
  my $html = $self->ua->get("https://en.wikipedia.org/wiki/User:HBC_AIV_helperbot/blank")->decoded_content();
  if ($html =~ m|\"wgUserName\":null|) { delete $self->{'logged_in'}; return 0 }
  return 1;
}

sub get {
  my $self = shift;
  my $page = shift;
  my $data = $self->api2data( 'query', [ meta => 'tokens', prop => 'revisions', rvprop => 'content|timestamp', titles => $page ] );
  my $content = $data->{query}{pages}{page}{revisions}{rev}{content};
  my $edit_token = $data->{query}{tokens}{csrftoken};
  return ( wantarray ) ? ( $content, $data ) : $content;
}

sub put {
  my $self    = shift;
  my $page    = shift;
  my $summary = shift;
  my $content = shift;
  my $minor   = shift;
  my $read    = shift;
  my $bot     = shift;
  my $post = [
    #assert        => 'bot',
    title         => $page,
    summary       => $summary || 'Missing edit summary, this is a bug',
    text          => $content, 
    bot           => 1,
    basetimestamp => $read->{query}{pages}{page}{revisions}{rev}{timestamp},
    token         => $read->{query}{tokens}{csrftoken},
  ];
  print "Editing $page";
  push( @{ $post }, 'minor', 1 ) if ( $minor );
  my $data = $self->api2data( 'edit', $post );
  if( $data->{error}{code} eq 'assertbotfailed' ) {
    warn "Bot assertion failed, logging in again";
    my $data = $self->login();
  }
  #print Dumper { edit => $post };
  return $data;
}

sub api2data {
  my $self = shift;
  my $action = shift;
  my $post = shift;
  print Dumper { $action => $post } if $ENV{DEBUG} > 1;
  my $xml = $self->ua->request(POST 'https://en.wikipedia.org/w/api.php?action='.$action.'&format=xml', $post)->decoded_content();
  eval{$xml = XMLin($xml)};
  if ($@) {
    warn $@;
    return;
  }
  print Dumper { $action => $xml } if $ENV{DEBUG};
  return $xml;
}

1;