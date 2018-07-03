Schedule tasks!

General flow:
1. A new task is added to the handler. It is then put in the 
   task store and in the scheduler. The scheduler checks if 
   the task should be executed before any tasks it is currently waiting on. If it is,
   the new task is scheduled. If not, it will be picked up later from the store.

2. The scheduler waits on the next tasks to be executed. Once they are ready, it informs the handler, by passing on the
   tasks. The handler then sends them to the task sender, and updates their status.
   If a task should be repeated its' next execution time is updated. If not, it is marked as done.

3. The task sender then dispatches the tasks to consumers (like email senders, scheduled processes, etc'). 

# Requirements
- Python 3.6 or higher.

# Running the example

```python example.py```

# What's missing
- An API for tasks to be added externally.
- An implemented task sender (should be with RabbitMQ).
- Another implementation for the task store based on Redis (currently using SQLite).