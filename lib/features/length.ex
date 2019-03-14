defmodule Features.Length do
  def extract(_data, html) do
    String.length(html)
  end
end
