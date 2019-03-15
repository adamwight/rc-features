defmodule Features.RevisionHtml do
  # Depends on an edit event
  def calculate(edit) do
    html = WikiRestbase.get_revision_html(edit["server_url"], edit["title"], edit["revision"]["new"])
  end
end
