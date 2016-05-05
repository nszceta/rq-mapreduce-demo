# the code will not run with concurrency < 2 due to queue deadlock
rq worker -q mapreduce &
for i in {1..4}
do
    echo launching worker $i
    rq worker -q mapreduce_c &
done
