"""Packaged documentation for CLI and MCP consumers."""

from importlib.resources import files

__all__ = ["guide"]


def guide(section=None) -> str:
    """Return the full Spark guide or a named Markdown section."""
    text = files("streamwright.spark").joinpath("guide.md").read_text(encoding="utf-8")
    if section is None:
        return text
    title = str(section).strip().lower()
    blocks = text.split("\n## ")
    for block in blocks[1:]:
        heading, _, _content = block.partition("\n")
        if heading.strip().lower() == title:
            return "## " + block
    raise KeyError("no Spark guide section %r" % section)
