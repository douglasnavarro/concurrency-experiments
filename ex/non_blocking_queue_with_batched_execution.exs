#! /usr/bin/env elixir

Mix.install([:req])

defmodule Queue do
  defstruct [:threshold, :pending_tasks, :failed_tasks]

  def new(threshold \\ 5) do
    {:ok, pending_tasks_agent} = Agent.start_link(fn -> [] end, name: PendingTasks)
    {:ok, failed_tasks_agent} = Agent.start_link(fn -> [] end, name: FailedTasks)

    %__MODULE__{
      threshold: threshold,
      pending_tasks: pending_tasks_agent,
      failed_tasks: failed_tasks_agent
    }
  end

  def report(%__MODULE__{} = queue) do
    queue
    |> Map.from_struct()
    |> Map.put(:pending_tasks, Agent.get(queue.pending_tasks, & &1))
    |> Map.put(:failed_tasks, Agent.get(queue.failed_tasks, & &1))
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
    with :ok <- Agent.update(queue.pending_tasks, fn tasks -> tasks ++ [task] end) do
      IO.inspect(task, label: :enqueued_task)

      case Agent.get(queue.pending_tasks, & &1) do
        tasks when length(tasks) >= queue.threshold ->
          IO.inspect(Enum.map(tasks, & &1.id), label: :flushing_queue)

          results = run_tasks(tasks)

          Agent.update(queue.pending_tasks, fn _ -> [] end)

          Agent.update(queue.failed_tasks, fn failed_tasks ->
            failed_tasks ++ Enum.filter(results, &(!&1.succeeded))
          end)

        _tasks ->
          :ok
      end
    else
      error ->
        IO.puts("Unable to enqueue task, error: #{error}")
        :error
    end

    queue
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
|> Enum.map(fn t -> NonBlockingQueueWithBatchedExecution.enqueue(queue, t) end)

Queue.report(queue)
