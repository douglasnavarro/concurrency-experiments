from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import queue
from random import randint
import threading
import time
from typing import List

import requests


@dataclass
class Task:
    id: str
    name: str
    url: str


class NonBlockingQueue:
    """
    Similar to QueueWithBatchedConcurrentExecution, but while it executes batch
    it still allows enqueueing new tasks.

    Internally, relies on built-in queue.Queue.

    Instead of checking for queue size over threshold on enqueue,
    we have a consumer thread that keeps looking at the size of the queue
    and flushes it.
    """

    def __init__(self, threshold: int) -> None:
        self.threshold = threshold
        self.queue = queue.Queue()
        self.tasks: List[Task] = []
        self.tasks_lock = threading.Lock()

        self.failed_tasks: List[Task] = []
        self.failed_tasks_lock = threading.Lock()

        self.consumer_thread = threading.Thread(target=self._flush)
        self.consumer_thread.start()

    def enqueue(self, task: Task) -> None:
        print(f"Enqueueing {task.id}")
        self.queue.put(task)

    def _flush(self) -> None:
        while True:
            if self.queue.qsize() >= self.threshold:
                print(f"Flushing queue")
                tasks = [self.queue.get() for _ in range(self.threshold)]
                with ThreadPoolExecutor(max_workers=self.threshold) as executor:
                    executor.map(self._run_task, tasks)
            else:
                time.sleep(0.1)

    def _run_task(self, task: Task) -> None:
        try:
            print(f"Running task {task.id}")
            response = requests.get(task.url)
            response.raise_for_status()
        except Exception as e:
            print(f"Task failed: {task.id} - {str(e)}")
            with self.failed_tasks_lock:
                self.failed_tasks.append(task)
        finally:
            with self.tasks_lock:
                self.tasks = [
                    outstanding_task
                    for outstanding_task in self.tasks
                    if outstanding_task.id != task.id
                ]
            self.queue.task_done()

    def report(self) -> dict:
        """Get current status of the queue."""
        return {
            "pending_tasks": len(self.tasks),
            "failed_tasks": len(self.failed_tasks),
            "threshold": self.threshold,
            "failed_task_ids": [task.id for task in self.failed_tasks],
        }


def generate_random_task() -> Task:
    random_id = randint(0, 100)
    random_task_id = f"task-{random_id}"
    status = 200 if random_id % 2 == 0 else 404
    return Task(
        **{
            "id": random_task_id,
            "name": random_task_id,
            "url": f"http://httpbin.org/status/{status}",
        }
    )


def generate_task(id: int) -> Task:
    status = 200 if id % 2 == 0 else 404
    return Task(
        **{
            "id": id,
            "name": id,
            "url": f"http://httpbin.org/status/{status}",
        }
    )


task_count = 100
tasks = [generate_task(id) for id in range(task_count)]
queue = NonBlockingQueue(10)

for task in tasks:
    queue.enqueue(task)
    time.sleep(0.1)

time.sleep(5)

print(queue.report())
