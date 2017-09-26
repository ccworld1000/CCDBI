#!/usr/bin/perl
#
#  Created by CC on 2017/09/05.
#  Copyright © 2017 - now  CC | ccworld1000@gmail.com . All rights reserved.
#  https://github.com/ccworld1000/CCDBI

use v5.10;
use DBI;
use strict;
use File::Copy;
use JSON;

##################################################################

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
    
    my $debug = 0;
    if ($debug) {
        say "Right Args $argCount count";
    }
    
    my ($defaultDB, $defaultTable, $createSQL, $dropSQL) = @_;
    
    my $newdb = $defaultDB;
    $newdb =~ s/^/AA/g;
    
    copy ($defaultDB, $newdb) or die "copy $defaultDB to $newdb failed";

    if ($debug) {
        say $newdb;
    }
    
    my %attr = (
        RaiseError => 1,
	HandleError => sub {
		my $error = shift;
		say "[CC Error] : $error";
		return 1;
	},
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
    my $hkMarkType = qq (111111);
    
    my $insertSQL;
    
    my $count = 0;
    
    my $now = time();
    $newDBH->begin_work();
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
            $markType = $hkMarkType;
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
    
    $newDBH->commit ();
    
    my $lastTime = time();
    my $delta = $lastTime - $now;

    say "Convert $count count OK! [$delta s]";
    
    $sth->finish ();
    $dbh->disconnect();
    $newDBH->disconnect();
}

##################################################################


##################################################################

# convert JSON to DB

sub convertJSON2DB {
    say "convertJSON2DB ......";
    
    my $argCount = @_;
    
    if ($argCount != 6) {
        say q{arg error: need 6 args ($defaultDB, $defaultTable, $jFile, $createSQL, $dropSQL, $insertSQL)};
    }
    
    my $debug = 1;
    if ($debug) {
        say "Right Args $argCount count";
    }
    
    my ($defaultDB, $defaultTable, $jFile, $createSQL, $dropSQL, $insertSQL) = @_;
    
    my $newdb = $defaultDB;
    
    say "@_";

    my $content;
    open JFILE, "<", $jFile or die "can not open file : $!";
    $content = <JFILE>;
    close JFILE;
    
    my $json = JSON->new;
    my @data = $json->decode ($content);
    say "@data";
    

    my $usMarkType = "222222";
    my $hkMarkType = "111111";
    my $markType = "";
    
    my %attr = (
        RaiseError => 1,
        HandleError => sub {
            my $error = shift;
            say "[CC Error] : $error";
            return 1;
        },
    )  or die $DBI::errstr;;
    
    my $seq = 1;
    my $empty = "";
    my $newDBH = DBI->connect(
        "dbi:SQLite:dbname=$defaultDB",
        $empty,
        $empty,
        \%attr,
    );
    
    $newDBH->do ($dropSQL);
    $newDBH->do ($createSQL);

    my $now = time();
    $newDBH->begin_work();
    for my $item (@data) {
        my @list = @$item;
        for my $i (@list) {
            my %hash = %$i;
            my $code = $i->{code};
            my $cnName = $i->{"cnName"};
            my $dataType = $i->{"dataType"};
            my $enName = $i->{"enName"};
            my $cnSpell = $i->{"cnSpell"};
            my $cnSpellAbbr = $i->{"cnSpellAbbr"};
            my $ftName = $i->{"ftName"};
            my $pureCode = $code;
            
            if ($dataType =~ /^1/) {
                $markType = $hkMarkType;
                $pureCode =~ s/.hk//gi;
                say "[$seq] [CC HK] pureCode : " . $pureCode;
            } elsif ($dataType =~ /^2/) {
                $markType = $usMarkType;
                say "[$seq] [CC US] pureCode : " . $pureCode
            }
            
            $seq++;
            
            my @dataItems = ($code, $cnName, $dataType, $enName, $cnSpell, $cnSpellAbbr, $ftName, $pureCode, $markType);
            say $code . " " . $cnName . "@dataItems";
            
            $insertSQL = sprintf "INSERT INTO %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", $newDBH->quote_identifier($defaultTable),  $newDBH->quote($code), $newDBH->quote($cnName), $newDBH->quote($dataType), $newDBH->quote($enName), $newDBH->quote($cnSpell), $newDBH->quote($cnSpellAbbr), $newDBH->quote($ftName), $newDBH->quote($pureCode), $markType or die $DBI::errstr;
            
            $newDBH->do ($insertSQL);
        }
    }

    $newDBH->commit();
    
    $newDBH->disconnect();
    
    my $lastTime = time();
    my $delta = $lastTime - $now;
    
    say "Convert $seq count OK! [$delta s]";
}

##################################################################



1;
