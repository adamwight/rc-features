defmodule DependencyVisitor do
  @moduledoc """
  Build a data set by following digraph edges and executing nodes.
  """

  @spec visit_dependencies(:digraph, atom, fun) :: nil
  def visit_dependencies(graph, context, executor) do
    # Find and begin with the current roots, which have no dependencies
    # TODO: or for which inputs are ready.
    # FIXME: alternatively, require a single root which demultiplexes inputs.
    # TODO: we can scan in topsort order
    available_tasks = find_roots(graph)

    stream = Task.async_stream(available_tasks, &DependencyVisitor.visit_vertex(&1, graph, executor, context), ordered: false)
    Stream.run(stream)

    # bubble up to highest levels as results become available.
    # available.
  end

  @spec find_roots(:digraph) :: list
  def find_roots(graph) do
    # TODO
  end

  @spec visit_vertex(any, :digraph, fun, atom) :: nil
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

  @spec check_ready_downstreams(:digraph, Feature, atom) :: list
  def check_ready_downstreams(graph, vertex, context) do
    downstreams = :digraph.out_neighbors(graph, vertex)
    Enum.flat_map(downstreams, fn downstream_vertex ->
      {in_neighbors, available_inputs} = collect_inputs(graph, downstream_vertex, context)
      if length(available_inputs) == length(in_neighbors) do
        # All data is present, this vertex is ready for execution.
        [downstream_vertex]
      end
    end)
  end

  @spec collect_inputs(:digraph, Feature.t(), atom) :: {list, list}
  defp collect_inputs(graph, vertex, context) do
    upstreams = :digraph.in_neighbors(graph, vertex)
    # Get upstream results.
    input_data = upstreams.flat_map(fn upstream_vertex -> :ets.lookup(context, upstream_vertex) end)
    {upstreams, input_data}
  end
end
