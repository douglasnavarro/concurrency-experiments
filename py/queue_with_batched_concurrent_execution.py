from __future__ import annotations
from concurrent.futures import wait, ALL_COMPLETED, ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Tuple
from random import randint
import requests


@dataclass
class Task:
    id: str
    name: str
    url: str


class QueueWithBatchedConcurrentExecution:
    """
    - Runs tasks concurrently in batches of pre-determined size.
    - Adding new tasks is blocked until the last batch finished runinng.
    - Ensures there are no inconsistencies by avoiding mutation in the internal tasks state.
    - Assumes task ids are unique to avoid inconsistencies.
    """

    def __init__(self, threshold: int, concurrent: bool = False) -> None:
        self.threshold = threshold
        self.tasks: List[Task] = []
        self.failed_tasks: List[Task] = []
        self.concurrent = concurrent

        if concurrent:
            self.threadPool = ThreadPoolExecutor(5)
        else:
            self.threadPool = None

    def enqueue(self, task: Task) -> None:
        print(f"Enqueueing {task.id}")
        self.tasks.append(task)
        if self._is_over_threshold():
            self._flush()

    def _flush(self) -> None:
        if self.concurrent:
            futures = [
                self.threadPool.submit(self._run_task, task) for task in self.tasks
            ]
            # Queue is blocked while all tasks in the batch run
            # Because _flush is called in enqueue,
            # enqueueing new tasks also ends up blocked.
            wait(futures, return_when=ALL_COMPLETED)

        else:
            for task in self.tasks[:]:
                self._run_task(task)

    def _run_task(self, task: Task) -> Tuple[Task, bool]:
        try:
            print(f"Running task {task.id}")
            response = requests.get(task.url)
            response.raise_for_status()
            self.tasks = [
                outstanding_task
                for outstanding_task in self.tasks
                if outstanding_task.id != task.id
            ]
            return (task, True)
        except Exception as e:
            print(f"Task failed: {task.id} - {str(e)}")
            self.failed_tasks.append(task)
            self.tasks = [
                outstanding_task
                for outstanding_task in self.tasks
                if outstanding_task.id != task.id
            ]
            return (task, False)

    def _is_over_threshold(self) -> bool:
        return self._current_size() >= self.threshold

    def _current_size(self) -> int:
        return len(self.tasks)

    def report(self) -> dict:
        """Get current status of the queue."""
        return {
            "pending_tasks": len(self.tasks),
            "failed_tasks": len(self.failed_tasks),
            "threshold": self.threshold,
            "task_ids": [task.id for task in self.tasks],
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
queue = QueueWithBatchedConcurrentExecution(10, concurrent=True)

for task in tasks:
    queue.enqueue(task)


print(queue.report())
