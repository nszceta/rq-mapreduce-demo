from redis import StrictRedis
from rq import Queue
import random
import time
from toolz.itertoolz import partition_all, concat


def _reduce(*mapped):
    """ Reduce worker """
    return list(concat(mapped))


def _map(data):
    print(data)
    """ Map worker """
    results = []
    for chunk in data:
        results.append(sum(chunk))
    return results


def mapreduce(chunk_size):
    """ A long running task which splits up the input data to many workers """
    # create some sample data for our summation function
    data = []
    for i in range(1000):
        x = []
        for j in range(random.randrange(10) + 5):
            x.append(random.randrange(1000))
        data.append(x)

    # break up our data into chunks and create a dynamic list of workers
    print('preparing map')
    q = Queue('one', connection=StrictRedis())
    chunk_jobs = []
    for chunk in partition_all(chunk_size, data):
        chunk_jobs.append(
            q.enqueue_call(func=_map, args=(chunk,)))
    print('running map')
    while not all((job.is_finished for job in chunk_jobs)):
        time.sleep(0.5)
    print('preparing reduce')
    reduce_job = q.enqueue_call(
        func=_reduce, args=(tuple(job.result for job in chunk_jobs)))
    print('running reduce')
    while not reduce_job.is_finished:
        time.sleep(0.1)

    return reduce_job.result
