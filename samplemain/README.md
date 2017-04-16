Test for compact log replay

DIR=/tmp/samplerun

rm -Rf $DIR && mkdir $DIR

./samplemain -dir $DIR

Stop the compaction when it outputs a few tables but is not done yet.

Then do NOT clean the directory and run the main again.

You should see something like:

CLEANUP: Undo compaction ID 4
CLEANUP: Del /tmp/samplerun/table_0000000014
CLEANUP: Del /tmp/samplerun/table_0000000015
