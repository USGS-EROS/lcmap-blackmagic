from blackmagic import cfg
from blackmagic import db
from multiprocessing import Pool
from multiprocessing import Process
from multiprocessing import Manager


def queue():
    return Manager().Queue()


def workers(cfg):
    return Pool(cfg['cpus_per_worker'])


def writers(cfg, q):
    w = [Process(name='cassandra-writer[{}]'.format(i),
                 target=db.writer,
                 kwargs={'cfg': cfg, 'q': q},
                 daemon=False)
         for i in range(cfg['cassandra_concurrent_writes'])]
    [writer.start() for writer in w]
    return w
