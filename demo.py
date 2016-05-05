from redis import StrictRedis
from rq import Queue
from rq.job import Job
from rq_mapreduce import mapreduce
import time

q = Queue('two', connection=StrictRedis())


def create_work(chunk_size):
    """ A fast task for initiating our map function """
    return q.enqueue_call(func=mapreduce, args=(chunk_size,)).id


def get_work(job_id):
    """ A fast task for checking our map result """
    my_job = Job(id=my_id, connection=StrictRedis())
    while not my_job.is_finished:
        time.sleep(0.1)

    print(my_job.result)


if __name__ == '__main__':
    my_id = create_work(chunk_size=4)
    print(my_id)
    get_work(my_id)
