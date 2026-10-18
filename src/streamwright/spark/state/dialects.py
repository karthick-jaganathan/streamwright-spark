"""The two SQL dialects the bookkeeping store runs on. PROPOSED. A dialect is data plus one text transform: no branching."""


__all__ = ["SqliteDialect", "PostgresDialect"]


class SqliteDialect(object):
    name = "sqlite"
    has_schemas = False
    share_lock = ""                   # sqlite serializes writers itself
    update_lock = ""

    def placeholders(self, text):
        return text


class PostgresDialect(object):
    name = "postgres"
    has_schemas = True
    share_lock = " FOR SHARE"         # blocks a lease takeover until the writer commits
    update_lock = " FOR UPDATE"

    def placeholders(self, text):
        return text.replace("?", "%s")
