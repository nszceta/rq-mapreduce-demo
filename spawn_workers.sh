
# the code will not run with concurrency < 2 due to queue deadlock
for i in {1..2}
do
    ( rq worker -q two one & )
done
