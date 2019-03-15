defmodule DependencyVisitor do
  @moduledoc """
  Build a data set by following digraph edges and executing nodes.
  """
  def visit_dependencies(graph, context, executor) do
    # FIXME: Initial parameters should be explicitly wired into the graph.
    available_tasks = find_leaves(graph)

    stream = Task.async_stream(available_tasks, &DependencyVisitor.visit_vertex(&1, graph, executor, context), ordered: false)
    Stream.run(stream)

    # bubble up to highest levels as results become available.
    # available.
  end

  @spec visit_vertex(vertex, digraph, fun, ets) :: nil
  def visit_vertex(vertex, graph, executor, context) do
    {_, input_data} = collect_inputs(graph, vertex, context)
    # FIXME: multiple input edge data must be distinguished by pair keying.
    # Execute this node.
    output = executor.(input_data, vertex, context)
    # TODO: pluggable, tee to an external result store such as thrift
    # TODO: check success
    :ets.insert(context, {vertex, output})
    # for each of my out-neighbors, if all of your input nodes are ready, then recurse.
    ready_downstreams = check_ready_downstreams(graph, vertex, context)
    # FIXME: race condition or not?
    Task.async_stream(ready_downstreams, fn downstream_vertex ->
      visit_vertex(downstream_vertex, graph, executor, context)
    end)
  end

  def check_ready_downstreams(graph, vertex, context) do
    downstreams = :digraph.out_neighbors(graph, vertex)
    Enum.flat_map(downstreams, downstream_vertex ->
      {in_neighbors, available_inputs} = collect_input_data(graph, downstream_vertex, context)
      if length(available_inputs) == length(in_neighbors) do
        # All data is present, this vertex is ready for execution.
        [downstream_vertex]
      end
  end

  defp collect_inputs(graph, vertex, context) do
    upstreams = :digraph.in_neighbors(graph, vertex)
    # Get upstream results.
    input_data = upstreams.flat_map(fn upstream_vertex -> :ets.lookup(context, upstream_vertex) end)
    {upstreams, input_data}
  end
end
