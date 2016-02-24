#!/usr/bin/perl

#open the file
#input: the directory containing the data files, the name of the directory containing output files
# open(IN, "out1_pareto") || die ("cannot open input file");

# open(OUT,'>','analyzed_data_out1_pareto');
opendir(DIR, $ARGV[0]) || die("Cannot open directory");
@inputfiles= readdir(DIR);
closedir(DIR);

#open(IN, $ARGV[0]) || die("cannot open input file");
#open(OUT,'>',$ARGV[1])|| die("cannot open output file");
foreach $fin (@inputfiles){
if($fin ne '.' and $fin ne'..')
{
	open(IN, $ARGV[0]."/". $fin) || die("cannot open input file");
	open(OUT,'>',$ARGV[1]."/".$fin)|| die("cannot open output file");
	 #extract the data
	 my $count = 0;	
	 my $last_ts = 0;
	 my $sum = 0;	
	 while($line=<IN>)
	 {
		#print "$line"; 
		chomp($line);
	
		if($line =~ /[^\[]*[\[]([^\]]+)[\]][^\[]*[\[]([^\]]+)[\]][^\n]*/)
		{
		
			#if($2<1000000000)
			#{	
				$count = $count + 1;	 		
				$last_ts = $1;
				$sum = $sum + $2; 		
			#}
	 	} 
		if($count == 10)
		{
			$avg = int($sum/$count) ;	   
			print OUT "$last_ts     $avg\n";		
			$count = 0;
			$sum = 0;
		}
	
	 }
	if($count > 0 )
		{
			$avg = int($sum/$count) ;	   
			print OUT"$last_ts     $avg\n";		
		}
	close(IN);
	close (OUT);
}
}
print "\n...finish analysing data \n";
