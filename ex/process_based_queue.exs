#! /usr/bin/env elixir

Mix.install([:req])

defmodule QueueTask do
  defstruct [:id, :url]
  def new(id, url), do: %__MODULE__{id: id, url: url}
end

defmodule QueueServer do
  def start_link(threshold \\ 5) do
    spawn_link(fn ->
      Process.register(self(), :queue_server)

      loop(%{
        threshold: threshold,
        pending_tasks: [],
        failed_tasks: [],
        processing: false
      })
    end)
  end

  # A BEAM process will always process a single message at a time in the
  # same order in which they've arrived.
  # This is a core concept to rely on when using processes in Elixir.
  defp loop(state) do
    receive do
      {:enqueue, task, caller_pid} ->
        new_state = handle_enqueue(state, task)
        send(caller_pid, {:enqueue_reply, :ok})
        loop(new_state)

      {:report, caller_pid} ->
        send(caller_pid, {:report_reply, state})
        loop(state)

      {:get_state, caller_pid} ->
        send(caller_pid, {:state_reply, state})
        loop(state)

      {:batch_completed, results} ->
        # Update state with completed batch results and check for more tasks
        IO.inspect("Received batch completion, checking for more tasks", label: :debug)

        # Update failed tasks with results
        failed_tasks = state.failed_tasks ++ Enum.filter(results, &(!&1.succeeded))
        updated_state = %{state | failed_tasks: failed_tasks, processing: false}

        # Check if more tasks accumulated while processing
        new_state = maybe_process_batch(updated_state)
        loop(new_state)
    end
  end

  defp handle_enqueue(state, task) do
    # Add task to pending list
    new_state = %{state | pending_tasks: state.pending_tasks ++ [task]}

    IO.inspect(task, label: :enqueued_task)

    # Check if we should process the batch
    maybe_process_batch(new_state)
  end

  defp maybe_process_batch(state) do
    if state.processing do
      # Already processing, don't start another
      state
    else
      # Because we allow enqueuing while processing a batch, we no longer
      # guarantee that batches will be the same size as threshold.
      # If a batch takes long to run and enqueing goes on normally,
      # the next batch will be larger.
      if length(state.pending_tasks) >= state.threshold do
        # Start processing in background with current tasks
        tasks_to_process = state.pending_tasks
        spawn(fn -> process_batch(tasks_to_process) end)
        %{state | pending_tasks: [], processing: true}
      else
        # Not processing
        state
      end
    end
  end

  defp process_batch(tasks_to_process) do
    IO.inspect(Enum.map(tasks_to_process, & &1.id), label: :flushing_queue)

    results = run_tasks(tasks_to_process)

    IO.inspect("Batch completed, sending results back to main process", label: :debug)

    # Send the results back to the main queue process
    send(Process.whereis(:queue_server), {:batch_completed, results})
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

defmodule ProcessBasedQueue do
  def init(threshold \\ 5) do
    QueueServer.start_link(threshold)
  end

  def enqueue(queue_pid, %QueueTask{} = task) do
    send(queue_pid, {:enqueue, task, self()})

    receive do
      {:enqueue_reply, :ok} -> :ok
    end
  end

  def report(queue_pid) do
    send(queue_pid, {:report, self()})

    receive do
      {:report_reply, state} ->
        state
        |> IO.inspect(label: :queue)

        :ok
    end
  end

  def get_state(queue_pid) do
    send(queue_pid, {:get_state, self()})

    receive do
      {:state_reply, state} -> state
    end
  end
end

### Manual Testing Section ###

queue_pid = ProcessBasedQueue.init(5)

tasks =
  1..25
  |> Enum.map(fn i ->
    QueueTask.new("t#{i}", "https://httpbin.org/status/#{if rem(i, 2) == 0, do: 200, else: 400}")
  end)

tasks
|> Task.async_stream(fn t ->
  ProcessBasedQueue.enqueue(queue_pid, t)
end)
|> Enum.to_list()

:timer.sleep(5_000)
ProcessBasedQueue.report(queue_pid)
