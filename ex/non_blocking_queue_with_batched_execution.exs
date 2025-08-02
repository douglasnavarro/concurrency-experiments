#! /usr/bin/env elixir

Mix.install([:req])

defmodule Queue do
  defstruct [:threshold, :pending_tasks, :failed_tasks, :processing]

  def new(threshold \\ 5) do
    {:ok, pending_tasks_agent} = Agent.start_link(fn -> [] end, name: PendingTasks)
    {:ok, failed_tasks_agent} = Agent.start_link(fn -> [] end, name: FailedTasks)
    {:ok, processing_agent} = Agent.start_link(fn -> false end, name: Processing)

    %__MODULE__{
      threshold: threshold,
      pending_tasks: pending_tasks_agent,
      failed_tasks: failed_tasks_agent,
      processing: processing_agent
    }
  end

  def report(%__MODULE__{} = queue) do
    queue
    |> Map.from_struct()
    |> Map.put(:pending_tasks, Agent.get(queue.pending_tasks, & &1))
    |> Map.put(:failed_tasks, Agent.get(queue.failed_tasks, & &1))
    |> Map.put(:processing, Agent.get(queue.processing, & &1))
    |> IO.inspect(label: :queue)

    :ok
  end
end

defmodule QueueTask do
  defstruct [:id, :url]
  def new(id, url), do: %__MODULE__{id: id, url: url}
end

defmodule NonBlockingQueueWithBatchedExecution do
  def init(threshold \\ 5) do
    Queue.new(threshold)
  end

  def enqueue(%Queue{} = queue, %QueueTask{} = task) do
    # Add task to pending list atomically
    Agent.update(queue.pending_tasks, fn tasks -> tasks ++ [task] end)

    IO.inspect(task, label: :enqueued_task)

    # Check if we should process the batch
    maybe_process_batch(queue)

    queue
  end

  defp maybe_process_batch(%Queue{} = queue) do
    # Use Agent.get_and_update to atomically check and potentially set processing flag
    Agent.get_and_update(queue.processing, fn is_processing ->
      if is_processing do
        # Already processing, don't start another
        {false, true}
      else
        # Check if we have enough tasks to process
        pending_tasks = Agent.get(queue.pending_tasks, & &1)

        if length(pending_tasks) >= queue.threshold do
          # Start processing in background
          Task.start(fn -> process_batch(queue) end)
          # Set processing flag
          {true, true}
        else
          # Not processing
          {false, false}
        end
      end
    end)
  end

  defp process_batch(%Queue{} = queue) do
    # Get all pending tasks and clear the list atomically
    tasks = Agent.get_and_update(queue.pending_tasks, fn tasks -> {tasks, []} end)

    IO.inspect(Enum.map(tasks, & &1.id), label: :flushing_queue)

    results = run_tasks(tasks)

    # Update failed tasks
    Agent.update(queue.failed_tasks, fn failed_tasks ->
      failed_tasks ++ Enum.filter(results, &(!&1.succeeded))
    end)

    # Reset processing flag
    Agent.update(queue.processing, fn _ -> false end)

    # Check if more tasks accumulated while processing
    maybe_process_batch(queue)
  end

  defp run_tasks(queue_tasks) do
    queue_tasks
    |> Task.async_stream(fn queue_task -> {queue_task.id, Req.get!(queue_task.url)} end)
    |> Enum.map(fn
      {:ok, {queue_task_id, %{status: 200}}} -> %{task_id: queue_task_id, succeeded: true}
      {:ok, {queue_task_id, _}} -> %{task_id: queue_task_id, succeeded: false}
      {:error, error} -> raise "Unknown error running task: #{error}"
    end)
  end
end

### Manual Testing Section ###

queue = NonBlockingQueueWithBatchedExecution.init(5)

tasks =
  1..25
  |> Enum.map(fn i ->
    QueueTask.new("t#{i}", "https://httpbin.org/status/#{if rem(i, 2) == 0, do: 200, else: 400}")
  end)

tasks
|> Task.async_stream(fn t ->
  NonBlockingQueueWithBatchedExecution.enqueue(queue, t)
end)
|> Enum.to_list()

:timer.sleep(5_000)
Queue.report(queue)
