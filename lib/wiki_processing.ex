defmodule WikiProcessing do
  require Logger

  def start(:normal, []) do
    WikiSSE.start_link(&WikiProcessing.process_event/1)
  end

  @doc """
  Called (in a new process) with each wiki event.  Side effects.
  """
  @spec process_event(struct) :: nil
  def process_event(message) do
    data = Poison.decode!(message.data)

    verbose = Application.fetch_env!(:recent_processing, :verbose)
    verbose and data |> Poison.encode!(pretty: true) |> Logger.debug

    #edit_features = Application.fetch_env!(:recent_processing, :edit_features)

    # TODO: config
    #graph = DotReader.load_dot(
    #graph = Application.fetch_env
    # FIXME: data -> ets context
    graph = sample_dag(data)
    visitor = &DependencyVisitor.visit_dependencies/3
    executor = &WikiProcessing.handle_vertex/3

    # TODO: new, uniquely-named table for each job.  Should be owned by a
    # supervisor which can retry and reuse incomplete results.
    # Primary storage backend should get syncs of the results, either
    # in-progress or at completion.
    state = :ets.new(:results, [write_concurrency: true])

    visitor.(graph, :results, executor)

    verbose and Logger.debug(state.tab2list())

    # TODO:
    # * ets for the results (but pluggable to also persist to hdfs)
    # * curses progress
  end

  @spec sample_dag(map) :: :digraph
  def sample_dag(event) do
    # TODO: initial inputs should be wired into a boundary rather than directly in the graph.
    graph = :digraph.new()
    event_spout = :digraph.add_vertex(graph, fn () -> [event] end)

    edits = :digraph.add_vertex(graph, Filters.EditEvent)
    :digraph.add_edge(graph, event_spout, edits)

    html = :digraph.add_vertex(graph, Features.RevisionHtml)
    :digraph.add_edge(graph, edits, html)

    graph
  end

  @doc """
  Execute a node.  This implementation expects the feature to have Feature behavior,

  We don't start a new process, that's the responsibility of our caller.

  Results are stored under the feature's name keyy
  """
  @spec handle_vertex(List, Feature, Atom) :: any
  def handle_vertex(input_data, feature, context) do
    case feature do
      fun ->
        # N-arity raw function
        feature.(input_data, context)
      Module ->
        # TODO: implements my local vertex behavior
        feature.calculate(input_data)
    end
  end
end
