defmodule EditEvent do
  def calculate(event) do
    case event["type"] do
      "edit" ->
        event
      _ ->
        nil
    end
  end
end
