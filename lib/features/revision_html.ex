defmodule Features.RevisionHtml do
  # Depends on an edit event
  @spec calculate(map) :: String.t()
  def calculate(edit) do
    WikiRestbase.get_revision_html(edit["server_url"], edit["title"], edit["revision"]["new"])
  end
end
