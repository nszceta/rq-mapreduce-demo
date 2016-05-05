# the code will not run with concurrency < 2 due to queue deadlock
rq worker -q mapreduce &
rq worker -q mapreduce_c
