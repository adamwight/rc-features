defmodule WikiProcessing do
  require Logger

  def start(:normal, []) do
    WikiSSE.start_link(&WikiProcessing.process_event/1)
  end

  @doc """
  Called (in a new process) with each wiki event.  Side effects.
  """
  @spec process_event() :: nil
  def process_event(message) do
    data = Poison.decode!(message.data)

    verbose = Application.fetch_env!(:recent_processing, :verbose)
    verbose and data |> Poison.encode!(pretty: true) |> Logger.debug

    edit_features = Application.fetch_env!(:recent_processing, :edit_features)

    # TODO: config
    #graph = DotReader.load_dot(
    #graph = Application.fetch_env
    # FIXME: data -> ets context
    graph = sample_dag(data)
    visitor = &WikiProcessing.visit_dependencies/3
    executor = &WikiProcessing.handle_vertex/1

    state = :ets.new(:results, [{write_concurrency: true}])

    visitor.(graph, state, executor)

    verbose and Logger.debug(state.tab2list())

    # TODO:
    # * ets for the results (but pluggable to also persist to hdfs)
    # * curses progress
  end

  def sample_dag(event) do
    graph = :digraph.new()
    event_spout = :digraph.add_vertex(graph, () -> [event])

    edits = :digraph.add_vertex(graph, Filters.EditEvent)
    :digraph.add_edge(graph, event_spout, edits)

    html = :digraph.add_vertex(graph, Features.RevisionHtml)
    :digraph.add_edge(graph, edits, html)

    graph
  end

  def extract_feature(feature, context) do
    result = feature.extract(context)
    Logger.debug("#{feature}: #{result}")
  end

  def handle_vertex(input_data, feature, context) do
    case feature do
      _ :: _ ->
        # 1-arity raw function
        feature.(input_data[1])
      Enum :: _ ->
        # N-arity raw function
        feature.(input_data)
      Module ->
        # TODO: implements my local vertex behavior
        feature.calculate(input_data)
    end
  end
end
