from redis import Redis
from rq import Queue, Connection
import random
import time
from toolz.itertoolz import partition_all, concat


def _reduce(*mapped):
    """ Reduce worker """
    return list(concat(mapped))


def _map(data):
    """ Map worker """
    results = []
    for chunk in data:
        results.append(sum(chunk))
    return results


def mapreduce(chunk_size):
    """ A long running task which splits up the input data to many workers """
    # create some sample data for our summation function
    data = []
    for i in range(10000):
        x = []
        for j in range(random.randrange(10) + 5):
            x.append(random.randrange(100))
        data.append(x)
    for row in data:
        print('input -> ' + str(row))

    # break up our data into chunks and create a dynamic list of workers
    chunk_jobs = []
    with Connection(Redis()):
        q = Queue()
        for chunk in partition_all(chunk_size, data):
            chunk_jobs.append(
                q.enqueue_call(func=_map, args=(chunk,)))
        while not all((job.is_finished for job in chunk_jobs)):
            time.sleep(0.1)

    with Connection(Redis()):
        q = Queue()
        reduce_job = q.enqueue_call(
            func=_reduce, args=tuple(job.result for job in chunk_jobs))
        while not reduce_job.is_finished:
            time.sleep(0.1)

    return reduce_job.result

