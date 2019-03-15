# TODO: move to wiki_elixir, support all API methods
defmodule WikiRestbase do
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
