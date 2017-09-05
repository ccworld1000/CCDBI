#!/usr/bin/perl
#
#  Created by CC on 2017/09/05.
#  Copyright © 2017 - now  CC | ccworld1000@gmail.com . All rights reserved.
#

use v5.10;
use strict;
use DBI;

##################################################################

# check is Support SQLite

sub supportSQLite {
    my $isSupportSQLite = 0;
    
    my @driver_names = DBI->available_drivers;
    my $dbnames = join (" ", @driver_names);

    if (!($dbnames =~ m/SQLite/i)) {
        warn "You must support DBD for SQLite";
        $isSupportSQLite = 0;
    } else {
        $isSupportSQLite = 1;
    }

    say "Support dbnames => [$dbnames]";
    
    $isSupportSQLite;
}

##################################################################



##################################################################

# convert DB to DB

sub convertDB2DB {
    say "convertDB2DB ......";

    my $argCount = @_;
    
    if ($argCount != 4) {
        say q{arg error: need 4 args ($defaultDB, $defaultTable, $createSQL, $dropSQL)};
    }
    
    my $debug = 1;
    if ($debug) {
        say "Right Args $argCount count";
    }
    
    my ($defaultDB, $defaultTable, $createSQL, $dropSQL) = @_;
    
    my $newdb = $defaultDB;
    $newdb =~ s/^/CC/g;
    
    if ($debug) {
        say $newdb;
    }
    
    my %attr = (
        RaiseError => 1,
    );
    
    my $empty = "";
    my $dbh = DBI->connect (
        "dbi:SQLite:dbname=$defaultDB",
        $empty,
        $empty,
        \%attr,
    ) or die $DBI::errstr;
    
    my $newDBH = DBI->connect (
        "dbi:SQLite:dbname=$newdb",
        $empty,
        $empty,
        \%attr,
    );
    
    $newDBH->do ($dropSQL);
    $newDBH->do ($createSQL);
    
    my $querySQL = qq {select * from '$defaultTable'};
    my $sth = $dbh->prepare ($querySQL);
    $sth->execute ();
    
    my @row;
    my $content;
    
    my $usMarkType = qq (222222);
    my $ukMarkType = qq (111111);
    
    my $insertSQL;
    
    my $count = 0;
    
    while (@row = $sth->fetchrow_array()) {
        if ($debug) {
            $content = join ("\t", @row);
        }
    
        my ($code, $cnName, $dataType, $enName, $cnSpell, $cnSpellAbbr, $ftName) = @row;
        
        my $markType;
        my $pureCode = $code;
        if ($dataType =~ m/^2{1,}/) {
            $markType = $usMarkType;
            $pureCode = $code;
        } elsif ($dataType =~ m/^1{1,}/) {
            $markType = $ukMarkType;
            $pureCode =~ s/\.hk//gi;
        }
        
        if ($debug) {
            say "[Convert -> $count] [$pureCode] :: $content";
        } else {
            say "[Convert -> $count]";
        }
        
        $insertSQL = sprintf "INSERT INTO %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", $newDBH->quote_identifier($defaultTable),  $newDBH->quote($code), $newDBH->quote($cnName), $newDBH->quote($dataType), $newDBH->quote($enName), $newDBH->quote($cnSpell), $newDBH->quote($cnSpellAbbr), $newDBH->quote($ftName), $newDBH->quote($pureCode), $markType or die $DBI::errstr;
        
        $newDBH->do ($insertSQL);
        
        $count++;
    }
    
    say "Convert $count count OK!";
    
    $sth->finish ();
    $dbh->disconnect();
    $newDBH->disconnect();
}

##################################################################

1;
