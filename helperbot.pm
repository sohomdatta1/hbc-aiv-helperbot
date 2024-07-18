#!/usr/bin/env perl
# This script is released under the GFDL license, see
# http://en.wikipedia.org/w/index.php?title=User:HBC_AIV_helperbot/source&action=history
# for a full list of contributors
use Cwd qw( abs_path );
use File::Basename qw( dirname );
use lib dirname( abs_path( $0 ) );

use strict;
use warnings;
use DateTime;
use DateTime::Format::Duration;
use mwAPI;
use Net::Netmask;
use POSIX qw(strftime);
use Time::Local;
use URI::Escape;
use Data::Dumper;

my $mw = mwAPI->new();

### Configuration ###
my $read_rate = 30;
my $write_rate = 30;

my (%pages_to_watch) =
 (
   #'Wikipedia:Administrator intervention against vandalism'      => $read_rate,
   #'Wikipedia:Administrator intervention against vandalism/TB2'  => $read_rate,
   #'Wikipedia:Usernames for administrator attention'             => $read_rate * 2, # lots of users slows it down, check less often
   #'Wikipedia:Usernames for administrator attention/Bot'         => $read_rate * 2, # lots of users slows it down, check less often
   'User:SodiumBot/sandy-boxy' => 10
 );

# Pattern to match examples used in the instructions
my $example_pattern = qr/(?:IP ?address|username)/i;

my @desired_parameters = qw(
  RemoveBlocked MergeDuplicates AutoMark FixInstructions AutoBacklog
);
### End Configuration ###

my $version_number = '2.0.33';
my $VERSION = "HBC AIV helperbot v$version_number";

my %special_ips;
my %notable_cats;
my $instructions = '';

local $SIG{'__WARN__'} = \&mywarn;
open(PASS,'password'); sysread(PASS, $mw->{password}, -s(PASS)); close(PASS);
open(USER,'username'); sysread(USER, $mw->{username}, -s(USER)); close(USER);

# The program runs in this loop which handles a queue of jobs.
my(@job_list);
my $timing = 0;

add_job( [ \&login ], 0 );
add_job( [ \&get_ip_list ], 0 );
add_job( [ \&get_instructions ], 0 );

foreach my $page (keys %pages_to_watch)
  {
  add_job([\&check_page, $page],$timing);
  $timing += 5;
  }

while (1)                               # Infinite loop, a serpent biting it's own tail.
  {
  sleep(1);                             # Important in all infinite loops to keep it calm
  my (@kept_jobs);                      # A place to put jobs not ready to run yet
  while (my $job = shift(@job_list))    # Go through each job pending
    {
    my($r_job , $timing) = @{$job};
    if ($timing < time())               # If it is time to run it then run it
      {
      if (ref($r_job) eq 'ARRAY')       # Callback style, reference an array with a sub followed by paramaters
        {
        my $cmd = shift(@{$r_job});
        &{$cmd}(@{$r_job});
        }
      elsif (ref($r_job) eq 'CODE')     # Otherwise just the reference to the sub
        {
        &{$r_job};
        }
      warn( "Loop: ".scalar(@job_list)." jobs in queue, ".scalar(@kept_jobs)." put aside.\n" ) if( $ENV{LOOP_DEBUG} );
      }
    else                                # If it is not time yet, save it for later
      {
      push(@kept_jobs , $job)
      }
    }
  push (@job_list , @kept_jobs);        # Keep jobs that are still pending
  }

###################
### SUBROUTINES ###
###################

sub add_job {
  my ($r_job , $timing) = @_;
  push (@job_list , [$r_job , (time()+$timing)]);
}

sub login {
  warn "$VERSION logging in to en.wikipedia.org...\n";
  my( $username, $password ) = @_;
  my $result = $mw->login( $username, $password );

  return $result;
}

sub get_ip_list
  {
  warn "Fetching Special IP list...\n";
  my $ip_table = $mw->get('User:HBC AIV helperbot/Special IPs');
  unless ($ip_table) {
    warn "Failed to load page - will try again in 2 minutes.\n";
    add_job([\&get_ip_list],120);
    return;
  }
  %special_ips = (); # Clear any old list
  foreach my $line (split("\n",$ip_table))
    {
    if ($line =~ m|^\* \[\[:Category:(.*?)\]\]$|)
      {
      $notable_cats{$1} = 1;
      next;
      }
    next unless ($line =~ m|^;(.*?):(.*)$|);
    my ($ip, $comment) = ($1, $2);
    next unless ($ip =~ m|^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?:/\d{1,2})?$|);
    $special_ips{$ip} = "This IP matches the mask ($ip) in my [[User:HBC AIV helperbot/Special IPs|special IP list]] which is marked as: \"$comment\"";
    }
  warn "Done, will check again in 10 minutes.\n";
  add_job([\&get_ip_list],600); # Run myself in 10 minutes
  }

sub get_instructions {
  warn "Fetching instructions...\n";
  my $content = $mw->get('Wikipedia:Administrator intervention against vandalism/instructions');
  unless ($content) {
    warn "Failed to load page - will try again in 2 minutes.\n";
    add_job([\&get_instructions],120);
    return;
  }
  $instructions = ''; # start with a clean slate
  my $keep = 0;
  foreach my $line (split("\n",$content)) {
    if (!$keep && $line =~ m/^<!-- HBC AIV helperbot BEGIN INSTRUCTIONS -->$/) {
      $keep = 1;
      next;
    } elsif ($keep && $line =~ m/^<!-- HBC AIV helperbot END INSTRUCTIONS -->$/) {
      $keep = 0;
    }
    next unless $keep;
    $instructions .= "$line\n";
  }
  chomp($instructions);
  warn "Done, will check again in 30 minutes.\n";
  add_job([\&get_instructions],1800);
}

sub check_instructions {
  my ($page, $content) = @_;

  unless ($content =~ m/\Q$instructions\E/s) {
    add_job([\&fix_instructions,$page],0);
    return 0;
  }
  return 1;
}

sub check_page {
  # Read the page and gather usernames, give each use a check_user job on the queue
  # Then add check_page to the queue scheduled for $read_rate seconds
  my $page = shift;
  # Get page, read only
  my $content =  $mw->get( $page );
  unless ($content && $content =~ m|\{\{((?:no)? ?admin ?backlog)\|bot=HBC AIV helperbot5\}\}|i)
    {
    warn "Could not find backlog tag, not doing anything: $page\n";
    add_job( [ \&check_page, $page ], $pages_to_watch{ $page } );
    return;
    }
  my $ab_current = $1;
  unless ($content && $content =~ m|<\!-- (?:HBC AIV helperbot )?v([\d.]+) ((?:\w+=\S+\s+)+)-->|i)
    {
    warn "Could not find parameter string, not doing anything: $page\n";
    add_job( [ \&check_page, $page ], $pages_to_watch{ $page } );
    return;
    }
  my ($active_version, $parameters) = ($1,$2);
  unless (check_version($active_version)) {
    warn "Current version $version_number not allowed by active version $active_version on $page! Will check again in $pages_to_watch{ $page } minutes.\n";
    add_job( [ \&check_page, $page ], $pages_to_watch{ $page } );  # Schedule myself 2 minutes later
    return;
  }
  my $params = parse_parameters($parameters);
  add_job( [ \&check_page, $page ], $pages_to_watch{ $page } );
  ($params->{'AutoBacklog'} = '') if ($params->{'AddLimit'} <= $params->{'RemoveLimit'});
  if ($params->{'FixInstructions'} eq 'on') {
    return unless check_instructions($page,$content);
  }
  my @content = split("\n",$content); # Split into lines
  my $report_count = 0;
  my (%user_count, @IP_comments_needed, $merge_called, $in_comment);
  foreach my $line (@content)
    {
    my $bare_line;
    ($in_comment,$bare_line, undef) = comment_handler($line, $in_comment);
    next if ($in_comment && ($line eq $bare_line));
    ($bare_line =~ m/{\{((?:ip)?vandal|userlinks|user-uaa)\|\s*(.+?)\s*\}}/i) || next(); # Go to next line if there is not a vandal template on this one.
    my $user = $2;                              # Extract username from template
    my $user2;
    if ($user =~ m/^((?:1|user)=)/i) {
      $user2 = $user;
      $user =~ s/^$1//i;
    }
    $report_count++;
    $user_count{$user}++;
    if (($user_count{$user} > 1) && !($merge_called) && ($params->{'MergeDuplicates'} eq 'on'))
      {
      warn "Calling merge because of $user on $page\n";
      add_job([\&merge_duplicate_reports,$page],0);
      $merge_called = 1;
      }
    add_job([\&check_user,$user,$page,$line],0) if ($params->{'RemoveBlocked'} eq 'on'); # Queue a check_user job for the user to run ASAP
    my(@cats) = check_cats($user);
    if (scalar(@cats))
      {
      $special_ips{$user} = 'User is in the '.((scalar(@cats) > 1) ? ('categories') : ('category')).': ';
      foreach (@cats)
        {
        $_ = '[[:Category:'.$_.'|'.$_.']]'
        }
      $special_ips{$user} .= join(', ',@cats);
      $special_ips{$user} .= '.';
      }
    if ($params->{'AutoMark'} eq 'on' && !$merge_called)
      {
      if ($line !~ m|<\!-- Marked -->|)
        {
        foreach my $mask (keys(%special_ips))
          {
          if ($mask =~ m|^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?:/\d{1,2})?$| && $user =~ m|^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$|) {
            if (Net::Netmask->new($mask)->match($user))
              {
              push (@IP_comments_needed, [\&comment_special_IP,$page,$user,$mask]);
              last; # only match one mask
              }
          } else {
            if ($mask eq $user) {
              push (@IP_comments_needed, [\&comment_special_IP,$page,$user,$mask]);
              last; # only match one mask
            }
          }
          }
        }
      }
    }
  foreach my $ra_param (@IP_comments_needed)  {
    add_job([@{$ra_param},$report_count],0);
  }
  if ($params->{'AutoBacklog'} eq 'on' && !$merge_called)
    {
   add_job([\&set_backlog,$page,$report_count,$params->{'AddLimit'},$params->{'RemoveLimit'}],0)
     if         ((($report_count >= $params->{'AddLimit'})    && ($ab_current eq 'noadminbacklog')) ||
                 (($report_count <= $params->{'RemoveLimit'}) && ($ab_current eq   'adminbacklog')));
    }
  return;
  }

sub check_version {
  my ($active_version) = @_;

  my @active_parts = split(/\./, $active_version);
  my @my_parts = split(/\./, $version_number);

  return 0 if scalar(@active_parts) > scalar(@my_parts); # should never happen

  foreach (@active_parts) {
    my $check_part = shift(@my_parts);
    last if $check_part > $_;
    next if $_ <= $check_part;
    return 0;
  }

  return 1;
}

sub parse_parameters {
  my ($parameters) = @_;
  my %result;
  foreach my $item (split(/\s+/, $parameters)) {
    my ($key, $value) = split(/=/, $item);
    $result{$key} = lc($value);
  }

  foreach (@desired_parameters) {
    $result{$_} ||= 'off';
  }

  if ($result{'AutoBacklog'} eq 'on') {
    $result{'AddLimit'} ||= 0;
    $result{'RemoveLimit'} ||= 0;
  }
  return \%result;
}

sub comment_handler {
  my ($line, $in_comment) = @_;
  my ($comment_starts, $comment_ends, $remainder) = (0,0,'');

  if ($in_comment) {
    # check if an opened comment ends in this line
    if ($line =~ m|-->|) {
      $line =~ s|(.*?-->)||;
      $in_comment = 0;
      $comment_ends = 1;
      $remainder = $1;
    }
  }

  # remove any self-contained comments
  $line =~ s|<!--.*?-->||g;

  if ($line =~ s|<!--.*||) {
    $in_comment = 1;
    $comment_starts = 1;
  }

  return (wantarray) ? ($in_comment, $line, $remainder) :
    $in_comment;
}

sub check_cats
  {
  my ($user) = @_;
  my (@response);
  my $data = $mw->api2data( 'query', [ prop => 'categories', titles => "User talk :$user" ] );
  return () unless( exists $data->{query}{pages}{page}{categories}{cl} );
  my $cat_list = $data->{query}{pages}{page}{categories}{cl};
  $cat_list = ( ( ref( $cat_list ) eq 'ARRAY' ) ? ( $cat_list ) : ( [ $cat_list ] ) );
  foreach my $cat ( @{ $cat_list } ) {
    $cat->{title} =~ s/^Category://;
    push(@response, $cat->{title}) if ( $notable_cats{ $cat->{title} } );
  }
  return @response;
  }

sub check_user {
  # Determine if the user is blocked, and if so gather information about the block
  # and schedule a remove_name job with all the information passed along
  my ($user,$page,$line) = @_;
  my $search_key = 'bkusers';
  if ($user =~ /^(?:\d{1,3}\.){3}\d{1,3}$/ or $user =~ /^(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}$/i) {
    $search_key = 'bkip';
  }
  my $data = $mw->api2data( 'query', [ list => 'blocks', bkprop => 'id|user|by|timestamp|expiry|flags|restrictions', $search_key => $user ] );
  my $block = $data->{query}{blocks}{block};
  return unless $block;
  return unless $block->{id}; # there are multiple blocks; don't touch the AIV report
  foreach ( keys( %{ $block } ) ) {
    $block->{$_} = 1 if ( !$block->{$_} && defined $block->{$_} )
  };
  foreach my $time ( qw( expiry timestamp ) ) {
    if      ( $block->{$time} =~ m|(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})| ) {
      $block->{$time.'_epoch'} = DateTime->new(year=>$1,month=>$2,day=>$3,hour=>$4,minute=>$5,second=>$6,time_zone=>'UTC');
    } elsif ( $block->{$time} eq 'infinity' ) {
      $block->{duration} = 'indef';
    } else { $block->{duration} = 'unknown' }
  }
  $block->{duration} ||= timeconv( $block->{expiry_epoch}, $block->{timestamp_epoch} );
  delete( $block->{expiry_epoch} ); delete( $block->{timestamp_epoch} );
  if ($block->{partial}) {
    if ($line !~ m/<!-- PBMarked -->/) {
      add_job([\&comment_partial_blocked,$page,$user,$block],0);
    }
  } else {
    my(@flags);
    push(@flags,'AO' ) if ($block->{anononly});
    push(@flags,'TPD') if (!$block->{allowusertalk});
    push(@flags,'ACB') if ($block->{nocreate});
    push(@flags,'EMD') if ($block->{noemail});
    push(@flags,'Partial') if ($block->{partial});
    my $block_type = '';
    $block_type = '[[User:HBC AIV helperbot/Legend|('.join(' ',@flags).')]]' if (scalar(@flags));
    add_job([\&remove_name,$user,$block->{user},$block->{by},$block->{duration},$block_type,$page],0);
  }
}

sub timeconv {
  my($expiry, $block_time)  = @_;
  my $duration = $expiry - $block_time;
  my $formatter = DateTime::Format::Duration->new(
    pattern => '%Y years, %m months, %e days, %H hours, %M minutes, %S seconds',
    normalize => 1,
    base => $block_time,
  );
  my %normalized = $formatter->normalize($duration);
  my @periods = ('years','months','days','hours','minutes','seconds');
  my $output;
  if ($normalized{'minutes'} || $normalized{'seconds'}) {
    $output = sprintf('until %s %s ', $expiry->ymd, $expiry->hms);
  } else {
    foreach (@periods) {
      $output .= sprintf('%s %s, ', $normalized{$_}, $_) if $normalized{$_};
      if ($normalized{$_} == 1) {
        my $singular = $_;
        $singular =~ s/s$//;
        $output =~ s/$_/$singular/;
      }
    }
    $output =~ s/, $/ /;
    # special cases
    my %special_cases = (
      '1 day, 7 hours '  => '31 hours ',
      '1 day, 12 hours '  => '36 hours ',
      '2 days, 12 hours '  => '60 hours ',
    );
    $output = $special_cases{$output} if( exists $special_cases{$output} );
  }
  return $output;
}

sub merge_duplicate_reports {
  my ($page_name) = @_;
  my( $page, $edit_token ) = $mw->get($page_name);
  return unless $page && $edit_token;
  my(@content) = split("\n",$page); # Split into lines
  my (@new_content, %user_table, $report_count, $in_comment);
  while (scalar(@content)) {
    my $line = shift(@content);
    my $bare_line;
    ($in_comment,$bare_line,undef) = comment_handler($line, $in_comment);
    next if $line eq "\n";
    if (($in_comment && ($line eq $bare_line)) || $bare_line !~ m/{\{((?:ip)?vandal|userlinks|user-uaa)\|\s*(.*?)\s*\}}/i) {
      push(@new_content,$line); next;
    }
    my $user = $2;
    if ($user =~ m/^((?:1|user)=)/i) {
      $user =~ s/^$1//i;
    }
    if ($user) {
      unless ($user_table{$user}) {
        push(@new_content,$line);
        $user_table{$user} = \$new_content[scalar(@new_content)-1];
        while ((scalar(@content)) && !($content[0] =~ m/{\{((?:ip)?vandal|userlinks|user-uaa)\|/i) && !($content[0] =~ m|<\!--|)) {
          my $comment = shift(@content);
          $in_comment = comment_handler($comment, $in_comment);
          ${$user_table{$user}} .= "\n$comment"
        }
        $report_count++;
      }
      else {
        $line =~ s|^\*||;
        $line =~ s/{\{((?:ip)?vandal|userlinks|user-uaa)\|\s*(.*?)\s*\}}//i;
        ${$user_table{$user}} .= "\n*:$line <small><sup>(Moved by bot)</sup></small>";
      }
    }
  }
  my $tally;
  $tally = 'Empty.' if ($report_count == 0);
  $tally ||= ($report_count.' report'.(($report_count > 1) ? ('s remaining.') : (' remaining.')));
  my $save_content = join("\n",@new_content);
  my $save_summary = "$tally Duplicate entries merged";
  $mw->put( $page_name, $save_summary, $save_content, 1, $edit_token );
}

sub comment_special_IP
  {
  my($page_name,$user,$mask,$report_count) = @_;
  my( $page, $edit_token) = $mw->get($page_name); # Get page read/write
  return unless $page;
  my(@content) = split("\n",$page); # Split into lines
  my (@new_content, $in_comment); # Place to put replacement content
  foreach my $line (@content) {
    $in_comment = comment_handler($line, $in_comment);
    if (($line =~ m|\Q$user\E|) && ($line =~ m/{\{((?:ip)?vandal|userlinks|user-uaa)/i))
      {
      return if ($line =~ m|<\!-- Marked -->|);
      $line .= ' -->' if $in_comment;
      $line .= ' <!-- Marked -->'."\n*:'''Note''': $special_ips{$mask} ~~~~";
      $line .= ' <!-- ' if $in_comment;
      }
    push(@new_content,$line);
  }
  my $tally;
  $tally = 'Empty.' if ($report_count == 0);
  $tally ||= ($report_count.' report'.(($report_count > 1) ? ('s remaining.') : (' remaining.')));
  my $save_content = join("\n",@new_content);
  my $save_summary = $tally." Commenting on $user: $special_ips{$mask}";
  $mw->put( $page_name, $save_summary, $save_content, undef, $edit_token );

  warn "$user matched $mask, marked as: $special_ips{$mask}\n";
  return 1;
  }

sub comment_partial_blocked
  {
  my($page_name,$user,$block) = @_;
  my( $page, $edit_token) = $mw->get($page_name); # Get page read/write
  return unless $page;
  my(@content) = split("\n",$page); # Split into lines
  my (@new_content, $in_comment); # Place to put replacement content
  my $report_count = 0;
  foreach my $line (@content) {
    my $bare_line;
    ($in_comment,$bare_line, undef) = comment_handler($line, $in_comment);
    if (not ($in_comment && ($line eq $bare_line))) {
      if (($line =~ m/{\{((?:ip)?vandal|userlinks|user-uaa)/i)) {
        $report_count++;
        if ($line =~ m|\Q$user\E|) {
          return if ($line =~ m|<\!-- PBMarked -->|);
          $line .= ' -->' if $in_comment;
          $line .= ' <!-- PBMarked -->'."\n*:'''Note''': $block->{user} is currently partial blocked. ~~~~";
          $line .= ' <!-- ' if $in_comment;
        }
      }
    }
    push(@new_content,$line);
  }
  my $tally;
  $tally = 'Empty.' if ($report_count == 0);
  $tally ||= ($report_count.' report'.(($report_count > 1) ? ('s remaining.') : (' remaining.')));
  my $save_content = join("\n",@new_content);
  my $save_summary = $tally." Commenting on $user\'s partial block";
  $mw->put( $page_name, $save_summary, $save_content, undef, $edit_token );

  warn "$user is partial blocked, marked\n";
  return 1;
  }

sub set_backlog
  {
  my ( $page_name, $report_count,$ab_add,$ab_remove) = @_;
  $report_count ||= '0';
  my( $page, $edit_token ) = $mw->get($page_name); # Get page read/write
  return unless $page;
  my(@content) = split("\n",$page); # Split into lines
  my(@new_content); # Place to put replacement content
  my $summary;
  foreach my $line (@content)
    {
    if ($line =~ m|^\{\{(?:no)?testadminbacklog\|bot=HBC AIV helperbot5\}\}|i)
      {
      my $tally;
      $tally = 'Empty.' if ($report_count == 0);
      $tally ||= ($report_count.' report'.(($report_count > 1) ? ('s remaining.') : (' remaining.')));
      if        ($report_count >= $ab_add)
        {
        warn "Backlog tag added to: $page_name\n";
        $summary = ($tally.' Noticeboard is backlogged.');
        $line =~ s|^\{\{noadminbacklog|\{\{adminbacklog|i;
        push (@new_content,$line);
        }
      elsif     ($report_count <= $ab_remove)
        {
        warn "Backlog tag removed from: $page_name\n";
        $summary = ($tally.' Noticeboard is no longer backlogged.');
        $line =~ s|^\{\{adminbacklog|\{\{noadminbacklog|i;
        push (@new_content,$line);
        }
      }
    else
      {
      push(@new_content,$line);
      }
    }
  my $content = join("\n",@new_content);
  return unless($content);
  $mw->put( $page_name, $summary, $content, 1, $edit_token );
  }

sub mywarn {
  my ($msg) = @_;
  if ($^O eq 'MSWin32')
    {
    CORE::warn($msg);
    }
  else
    {
    CORE::warn('['.strftime('%F %T UTC',gmtime()).'] '.$msg);
    }
}

sub fix_instructions {
  my ($page_name) = @_;
  my( $content, $edit_token ) = $mw->get($page_name);
  return unless $content;
  my $summary;
  if ($content =~ m|===\s*User-reported\s*===\n|s) {
    $content =~ s|<!-- HagermanBot Auto-Unsigned -->|RE-ADD-HAGERMAN|;
    my @content = split("\n", $content);
    my (@reports_to_move, $in_comment, $report_count, $msg);
    foreach my $line (@content) {
      my ($bare_line,$remainder);
      ($in_comment,$bare_line,$remainder) = comment_handler($line, $in_comment);
      if ($line =~ m/{\{((?:ip)?vandal|userlinks|user-uaa)\|\s*(?!$example_pattern)/i) {
        push(@reports_to_move, $line) if $in_comment;
        $report_count++;
      } elsif ($remainder =~ m/{\{((?:ip)?vandal|userlinks|user-uaa)\|\s*(?!$example_pattern)/i) {
        $remainder =~ s/-->//;
        push(@reports_to_move, $remainder);
      }
    }
    if ($content =~ m|===\s*User-reported\s*===\s+<!--|s) {
      $content =~ s:(===\s*User-reported\s*===\s+)<!--.*?(-->|$):$1$instructions:s;
      $msg = '';
    } else {
      $content =~ s|(===\s*User-reported\s*===\n)|$1$instructions\n|s;
      $msg = ' Old instructions not found, please check page for problems.';
    }
    my $remaining_text;
    if ($report_count) {
      $remaining_text = ($report_count > 1) ? "$report_count reports remaining." : "$report_count report remaining.";
    } else {
      $remaining_text = "Empty.";
    }
    if (@reports_to_move) {
      my $reports_moved = scalar(@reports_to_move);
      if ($reports_moved > 50) {
        $summary = "$remaining_text Reset [[WP:AIV/I|instruction block]], WARNING: tried to move more than 50 reports, aborting - check history for lost reports.$msg";
      } else {
        foreach my $report (@reports_to_move) {
          if ($report =~ m|RE-ADD-HAGERMAN|) {
            $report =~ s|RE-ADD-HAGERMAN|<!-- HagermanBot Auto-Unsigned -->|;
            $report =~ s|~~~~||;
          } else {
            $report =~ s|~~~~|~~~~ <small><sup>(Original signature lost - report made inside comment)</sup></small>|;
          }
          $content .= "$report\n";
        }
        $summary = "$remaining_text Reset [[WP:AIV/I|instruction block]], $reports_moved report(s) moved to end of page.$msg";
      }
    } else {
      $summary = "$remaining_text Reset [[WP:AIV/I|instruction block]].$msg";
    }
    $content =~ s|RE-ADD-HAGERMAN|<!-- HagermanBot Auto-Unsigned -->|;
      $mw->put( $page_name, $summary, $content, undef, $edit_token);
      warn "Reset instruction block: $page_name\n";
  } else {
    warn "FATAL ERROR: User-reported header not found on $page_name!  Sleeping 2 minutes.\n";
    unless ($content =~ m|<!-- HBC AIV helperbot WARNING -->|) {
      $content .= "<!-- HBC AIV helperbot WARNING -->\n";
      $summary = 'WARNING: User-reported header not found!';
      $mw->put( $page_name, $summary, $content, undef, $edit_token);
    }
    sleep(120);
    return;
  }
}

sub remove_name
  {
  my ($user,$block_target,$blocker,$duration,$block_type,$page_name) = @_;
  my( $page, $edit_token) = $mw->get($page_name); # Get page read/write
  return unless $page;
  my($ips_left,$users_left) =  ('0','0'); # Start these with 0 instead of undef
  my(@content) = split("\n",$page); # Split into lines
  my (@new_content, $found, $lines_skipped, $in_comment);
  while (scalar(@content)) {
    my $line = shift(@content);
    my ($bare_line,$remainder);
    ($in_comment,$bare_line,$remainder) = comment_handler($line, $in_comment);
    unless (!$in_comment && $line =~ m/{\{((?:ip)?vandal|userlinks|user-uaa)\|\s*(?:1=|user=)?\Q$user\E\s*\}}/i)
      {
      push(@new_content,$line);
      next if ($in_comment && ($line eq $bare_line));
      if($bare_line =~ m/{\{IPvandal\|/i)
        {
        $ips_left++;
        }
      if($bare_line =~ m/{\{(vandal|userlinks|user-uaa)\|/i)
        {
        $users_left++;
        }
      }
    else
      {
      $found = 1;
      push(@new_content,$remainder) if $remainder;
      while ((scalar(@content)) && !($content[0] =~ m/{\{((?:ip)?vandal|userlinks|user-uaa)\|/i) && !($content[0] =~ m|^<\!--|) && !($content[0] =~ m/^=/))
        {
        my $removed = shift(@content);
        if (length($removed) > 0) {
          $lines_skipped++;
          $in_comment = comment_handler($removed, $in_comment);
        }
        }
      }
  }
  $page = join("\n",@new_content);
  return unless($found);                # Cancel if could not find the entry attempting to be removed.
  return unless($page);    # Cancel if result would blank the page.
  my $length = ((defined($duration)) ? (' '.$duration) : (' '));
  $length = ' indef ' if (defined($duration) && $duration eq 'indef');
  my $tally;
  if ($ips_left || $users_left)
    {
    $tally = join(' & ',
    (
     (($ips_left) ? ($ips_left.' IP'.(($ips_left > 1) ? ('s') : (''))) : ()),
     (($users_left) ? ($users_left.' user'.(($users_left > 1) ? ('s') : (''))) : ()),
    )).' left.';
    }
  else
    {
    $tally = 'Empty.'
    }
  my $skipped = (($lines_skipped) ? (" $lines_skipped comment(s) removed.") : (''));
  my $rangecomment = ($user eq $block_target) ? "blocked" : "[[Special:Contributions/$block_target|$block_target]] rangeblocked";
  my $summary = $tally.' rm [[Special:Contributions/'.$user.'|'.$user.']] ('.$rangecomment.$length.'by an admin '.$block_type.'). '.$skipped;
  $mw->put( $page_name, $summary, $page, undef, $edit_token );
  warn "rm '$user': $page_name\n";
  sleep($write_rate);
}