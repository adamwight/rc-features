defmodule WikiProcessing do
  require Logger

  def start(:normal, []) do
    WikiSSE.start_link(&WikiProcessing.process_event/1)
  end

  @doc """
  Called (in a new process) with each wiki event.
  """
  def process_event(message) do
    data = Poison.decode!(message.data)

    verbose = Application.fetch_env!(:recent_processing, :verbose)
    verbose and data |> Poison.encode!(pretty: true) |> Logger.debug

    edit_features = Application.fetch_env!(:recent_processing, :edit_features)

    case data["type"] do
      "edit" ->
        # FIXME: Make this a feature step.  Read a .dot dependency graph and execute dynamically.
        html = get_revision_html(data["server_url"], data["title"], data["revision"]["new"])
        if html != nil do
          stream = Task.async_stream(edit_features, WikiProcessing, :extract_feature, [data, html], ordered: false)
          Stream.run(stream)
        end
      _ ->
        nil
    end
  end

  def extract_feature(feature, data, html) do
    result = feature.extract(data, html)
    Logger.debug("#{feature}: #{result}")
  end

  @spec get_revision_html(String, String, integer) :: nil | String
  def get_revision_html(server_url, title, revision) do
    url = URI.encode "#{server_url}/api/rest_v1/page/html/#{title}/#{revision}"
    response = HTTPotion.get url
    # TODO: handle errors
    if HTTPotion.Response.success? response do
      response.body
    end
  end
end
