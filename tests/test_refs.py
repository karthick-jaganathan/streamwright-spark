import datetime
import unittest

from streamwright.core.jobs.refs import RefError
from streamwright.core.jobs.refs import sql_literal as core_sql_literal
from streamwright.core.jobs.refs import substitute as core_substitute
from streamwright.spark.io.sql import quote_literal as spark_quote


def sql_literal(value):
    return core_sql_literal(value, quote_text=spark_quote)


def substitute(text, values, batch_id, batch_ts, limit):
    return core_substitute(text, values, {"batch.id": batch_id, "batch.ts": batch_ts}, limit, quote_text=spark_quote)


class Literals(unittest.TestCase):
    def test_strings_are_escaped(self):
        self.assertEqual(sql_literal("o'brien"), "'o''brien'")
        self.assertEqual(sql_literal("a\\b"), "'a\\\\b'")
        self.assertEqual(sql_literal("x') OR 1=1 --"), "'x'') OR 1=1 --'")

    def test_types(self):
        self.assertEqual(sql_literal(None), "NULL")
        self.assertEqual(sql_literal(True), "TRUE")
        self.assertEqual(sql_literal(5), "5")
        self.assertEqual(sql_literal(datetime.date(2026, 10, 17)), "DATE '2026-10-17'")
        with self.assertRaises(RefError):
            sql_literal(float("nan"))
        with self.assertRaises(RefError):
            sql_literal("a\x00b")
        with self.assertRaises(RefError):
            sql_literal(object())


class Substitute(unittest.TestCase):
    def test_lists_and_batch(self):
        values = {("t", "event_date"): [datetime.date(2026, 10, 16), datetime.date(2026, 10, 17)], ("t", "c"): []}
        text = "target.d IN ({{ t.event_date }}) AND target.c IN ({{ t.c }}) AND b = {{ batch.id }}"
        self.assertEqual(
            substitute(text, values, "b1", "ts", 10),
            "target.d IN (DATE '2026-10-16', DATE '2026-10-17') AND target.c IN (NULL) AND b = 'b1'",
        )

    def test_cap(self):
        with self.assertRaises(RefError):
            substitute("{{ t.c }}", {("t", "c"): list(range(11))}, "b", "ts", 10)

    def test_unresolved_is_an_error(self):
        with self.assertRaises(RefError):
            substitute("x = {{ nope.c }}", {}, "b", "ts", 10)


if __name__ == "__main__":
    unittest.main()
