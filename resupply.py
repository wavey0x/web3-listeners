"""Run an explicitly selected set of prepared Resupply workers."""

import argparse
import importlib
import logging
import os
import threading

from sqlite_store import Store
import notifications
import recovery

logger=logging.getLogger(__name__)
WORKERS={
    'rsup-incentives':'incentives.rsup_incentives',
    'yb-incentives':'incentives.yb_incentives',
    'dao':'data_fetchers.resupply_dao',
    'retention':'data_fetchers.resupply_retention',
}


def selected_workers(value):
    names=[name.strip() for name in (value or '').split(',')]
    if not all(name in WORKERS for name in names) or len(set(names))!=len(names):
        raise ValueError('Set YEARN_RESUPPLY_WORKERS to a nonempty, unique comma-separated selection of: '+', '.join(WORKERS))
    return names


def prepare_workers(store,names):
    entries=[]
    for name in names:
        module=importlib.import_module(WORKERS[name])
        def check(connection):
            if name in ('rsup-incentives','yb-incentives'):
                module.sqlite_worker.checkpoint(connection,module.PROTOCOL)
            else:
                module.checkpoint(connection)
            notifications.state(connection,name)
        store.read(check)
        entries.append((name,module.main))
    return entries


def run_worker(name,entry,stop):
    while not stop.is_set():
        try:
            entry()
            raise RuntimeError('Worker returned unexpectedly')
        except Exception as error:
            if not recovery.transient(error):
                logger.error('%s requires operator action (%s)', name, type(error).__name__)
                stop.set()
                return
            # RPC exceptions can contain credentials. Keep their details out of supervisor logs.
            logger.error('%s stopped (%s); restarting after 60 seconds',name,type(error).__name__)
            if stop.wait(60):
                return


def main():
    from dotenv import load_dotenv
    load_dotenv()
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers',default=os.environ.get('YEARN_RESUPPLY_WORKERS'))
    parser.add_argument('--check-config',action='store_true',help='validate the selected database state without starting RPC or workers')
    args=parser.parse_args()
    entries=prepare_workers(Store.from_env(),selected_workers(args.workers))
    if args.check_config:
        print('Prepared workers: '+', '.join(name for name,_ in entries))
        return
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    stop=threading.Event()
    threads=[threading.Thread(target=run_worker,args=(name,entry,stop),name=name,daemon=True) for name,entry in entries]
    # Validate every selected worker before starting any of them.
    for thread in threads:
        thread.start()
    try:
        while not stop.wait(10):
            if any(not thread.is_alive() for thread in threads):
                raise RuntimeError('A Resupply worker exited outside its restart handler')
    finally:
        stop.set()
    raise recovery.FatalError('A Resupply worker stopped; inspect its named error and repair before restarting')


if __name__=='__main__':
    recovery.entrypoint(main)
