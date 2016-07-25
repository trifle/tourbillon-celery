import logging
import time

from celery import Celery
from celery.events import EventReceiver

logger = logging.getLogger(__name__)


class TourbillonReceiver(EventReceiver):

    def __init__(self, stop_event, *args, **kwargs):
        super(TourbillonReceiver, self).__init__(*args, **kwargs)
        self.stop_event = stop_event

    @property
    def should_stop(self):
        # logger.debug('should stop checked')
        return not self.stop_event.is_set()


def get_celery_stats(agent):
    agent.run_event.wait()
    config = agent.config['celery']
    db_config = config['database']
    agent.create_database(**db_config)

    app = Celery(broker=config['broker'])
    state = app.events.State(max_tasks_in_memory=100)

    def handle_event(event):
        try:
            state.event(event)
            if event['type'].startswith('task-'):
                return handle_task_event(event)
        except:
            logger.exception('cannot handle celery event')

    def handle_task_event(event):
        try:
            # logger.debug('task event: %s', event)
            if 'uuid' in event:
                task = state.tasks.get(event['uuid'])
                # logger.debug('current task: %s', task)
                if task.state in ['SUCCESS', 'FAILURE', 'RETRY'] and \
                        task.name is not None:
                    runtime = task.runtime if task.runtime else 0
                    data = [{
                        'measurement': 'tasks',
                        'tags': {
                            'worker': task.worker.hostname,
                            'task_name': task.name,
                            'state': task.state,
                        },
                        'fields': {
                            'runtime': float(runtime),
                            'timestamp': float(task.timestamp),
                            'started': float(task.started)
                        }
                    }]
                    agent.push(data, db_config['name'])
        except:
            logger.exception('cannot parse task event')

    while agent.run_event.is_set():
        try:
            with app.connection() as connection:
                Receiver = app.subclass_with_self(TourbillonReceiver,
                                                  reverse='events.Receiver')
                recv = Receiver(agent.run_event, connection, handlers={
                    '*': handle_event,
                })

                logger.debug('start capturing events')
                recv.capture()
        except:
            logger.exception('event receiver failed: sleep 1 seconds')
            time.sleep(1)

    logger.debug('get_celery_stats exited')
