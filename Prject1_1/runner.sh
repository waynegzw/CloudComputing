#!/bin/bash

######################################################################
# Answer script for Project 1 module 1 Fill in the functions below ###
# for each question. You may use any other files/scripts/languages ###
# in these functions as long as they are in the submission folder. ###
######################################################################

# Write or invoke the code to perform filtering on the dataset. Redirect
# the filtered output to a file called 'output' in the current folder.
# HINT1: You should write a function to get the answer to each question.
# Do not just echo the answer.
# HINT2: Please remember if there is no entry that satisfies the filtering requirements,
# especially for Q5-Q8, your code should return 0 rather than nothing.


answer_0() {
        # Fill in this Bash function to filter the dataset and redirect the
        # output to a file called 'output'.
        # Example:
        # python filter.py > output
        awk '{if(NF > 3 && $1~/^en$/ && $2!~/^(Media\:|Special\:|Talk\:|User\:|User_talk\:|Project\:|Priject_talk\:|File\:|File_talk\:|MediaWiki\:|MediaWiki_talk\:|Template\:|Template_talk\:|Help\:|Help_talk\:|Category\:|Category_talk\:|Portal\:|Wikipedia\:|Wikipedia_talk\:)/ && $2!~/^[a-z]/ && $2!~/(\.jpg|\.gif|\.png|\.JPG|\.GIF|\.PNG|\.txt|\.ico)$/ && $2!~/^(404_error\/|Main_Page|Hypertext_Transfer_Protocol|Search)$/) {print $2 "\t" $3}}' pagecounts-20151201-000000 | sort -t $'\t' -k2,2nr > output

}

# How many lines (items) were originally present in the input file
# pagecounts-20151201-000000 i.e line count before filtering?
# Run your commands/code to process the dataset and echo a
# single number to standard output.
answer_1() {
	# count number of lines
        awk 'END{print NR}' pagecounts-20151201-000000

}

# Before filtering, what was the total number of requests made to all
# of Wikimedia (all subprojects, all elements, all languages) during
# the hour covered by the file pagecounts-20151201-000000?
# Run your commands/code to process the dataset and echo a
# single number to standard output.
# Hint: We want you to add up all the views that appear in the original file.
# Do not do any filtering.
answer_2() {
	# If the record has more than three elements, column 3 is the page access
	# If the record has equal or less than three elements, colum 2 is the page access 
	awk '{if(NF>3) {count+=$3;} else count+=$2};END{print count}' pagecounts-20151201-000000
}

# How many lines emerged after applying all the filters?
# Run your commands/code to process the dataset and echo a
# single number to standard output.
answer_3() {
	# Count lines of output
        awk 'END{print NR}' output
}

# What was the most popular article in the filtered output?
# Run your commands/code to process the dataset and echo a
# single word to standard output.
answer_4() {
	# The output is in descending order, so the first record is the most popular one
        awk '{if(NR==1) print $1}' output
}


# How many articles are there in the filtered dataset have titles that include "cloud"
# and "computing"? 
# Both strings above are case insensitive, but please match exactly the above strings.
# Example: "clouds" or "ecloud" should not be counted as exactly match.
# Run your commands/code to process the dataset and echo a
# single number to standard output.
answer_5 () {
	#  "cloud" and "computing" cannot follow or be followed by any letter
        awk '{if (tolower($1)~/(^|[^a-z])cloud([^a-z]+|([^a-z]+.*[^a-z]+))computing([^a-z]+|$)/) i++};END{print i}' output
}

# What is the number of views of the most popular movie in the filtered output?
# (Hint: Entries for movies have "film" in the article name).
# Run your commands/code to process the dataset and echo a
# single number to standard output.
answer_6() {
	# Filter the titles that includes "film" and find the one has the most page access
        awk '{if($1~/film/ && $2>i) i=$2};END{print i}' output
}

# How many articles have more than 2500 views and less than 3000 views
# in the filtered output?
# Run your commands/code to process the dataset and echo a single number
# to standard output.
answer_7() {
	# Filter the views between 2500 and 3000
        awk '{if($2>2500 && $2<3000) i++};END{print i}' output
}


# How many views are there in the filtered dataset for all articles that have titles
# that start with a single number (0-9) and then a character?
# Example that satisfied: en 3D_computer_graphics  54 3207024
# Example that not satisfied: en 88th_Academy_Awards    111 1869823
# Run your commands/code to process the dataset and echo a single number
# to standard output.
answer_8() {
	# Column 1 should start with single digit and folllowed by a letter
        awk '{if($1~/^([0-9][a-zA-Z])/) total+=$2};END{print total}' output
}


# In the filtered dataset, are the 2014 films more popular or the 2015 films more popular?
# Hint: "2014_film" and "2015_film" are the key words to search.
# Both strings above are case sensitive.
answer_9() {
        # The function should return either 2014 or 2015.
	# Count the total page views of 2014 and 2015 independently and then compare
        awk '{if($1~/2014_film/) i+=$2};{if($1~/2015_film/) j+=$2};END{if(i-j>0) {print "2014";} else print "2015"}' output
}




# DO NOT MODIFY ANYTHING BELOW THIS LINE
answer_0 &> /dev/null

echo "The results of this run are : "
echo "{"

if [ -f 'output' ]
then
        echo -en ' '\"answer0\": \"'output' file created\"
        echo ","
else
        echo -en ' '\"answer0\": \"No 'output' file created\"
        echo ","
fi

a1=`answer_1`
echo -en ' '\"answer1\": \"$a1\"
echo $a1 > .1.out
echo ","

a2=`answer_2`
echo -en ' '\"answer2\": \"$a2\"
echo $a2 > .2.out
echo ","

a3=`answer_3`
echo -en ' '\"answer3\": \"$a3\"
echo $a3 > .3.out
echo ","

a4=`answer_4`
echo -en ' '\"answer4\": \"$a4\"
echo $a4 > .4.out
echo ","

a5=`answer_5`
echo -en ' '\"answer5\": \"$a5\"
echo $a5 > .5.out
echo ","

a6=`answer_6`
echo -en ' '\"answer6\": \"$a6\"
echo $a6 > .6.out
echo ","

a7=`answer_7`
echo -en ' '\"answer7\": \"$a7\"
echo $a7 > .7.out
echo ","

a8=`answer_8`
echo -en ' '\"answer8\": \"$a8\"
echo $a8 > .8.out
echo ","

a9=`answer_9`
echo -en ' '\"answer9\": \"$a9\"
echo $a9 > .9.out
echo ""


echo  "}"

echo ""
echo "If you feel these values are correct please run:"
echo "./submitter -a andrewID -l language <python,bash,java>"
echo "You can use ./submitter -h to view the usage at any time."
