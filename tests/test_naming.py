import datetime
import unittest

from streamwright.core.jobs.naming import NamingError, parse_template, plan_names, sanitize

BATCH = "2026-10-17T23:59:30Z"  # just before midnight UTC


def render(text, values=None, columns=(), *, slash=False, batch_ts=BATCH, seq=0, driver_host="ip-10-0-0-1"):
    return parse_template(text, columns, slash, constants=("driver_host",)).render(
        values or {}, batch_ts, "b42", seq, {"driver_host": driver_host}
    )


class DateStyle(unittest.TestCase):
    def test_today_defaults_and_formats(self):
        self.assertEqual(render("orders_{today}.csv"), "orders_20261017.csv")
        self.assertEqual(render("{today:%Y-%m-%d}"), "2026-10-17")
        self.assertEqual(render("{now}"), "20261017T235930Z")
        self.assertEqual(render("{now:%F_%H%M%S}"), "2026-10-17_235930")
        with self.assertRaises(NamingError):  # %T would put colons in a name
            parse_template("{now:%T}")

    def test_offsets_like_date_dash_d(self):
        self.assertEqual(render("{today-1d}"), "20261016")
        self.assertEqual(render("{now+1h:%Y%m%d%H}"), "2026101800")  # crosses midnight
        self.assertEqual(render("{today-1w:%Y%m%d}"), "20261010")
        self.assertEqual(render("{now+1d-2h:%d%H}"), "1821")

    def test_gnu_no_pad_flag_and_names(self):
        self.assertEqual(render("{today:%-d_%b_%a}", batch_ts="2026-03-05T00:00:00Z"), "5_Mar_Thu")
        self.assertEqual(render("{today:%j_%u_%V}", batch_ts="2026-01-01T00:00:00Z"), "001_4_01")
        self.assertEqual(render("{now:%s}", batch_ts="1970-01-02T00:00:00Z"), "86400")

    def test_batch_time_is_pinned_not_the_clock(self):
        # the same batch rendered later gives the same name: a replay after midnight overwrites, never duplicates
        first = render("{today}", batch_ts=BATCH)
        self.assertEqual(first, render("{today}", batch_ts=datetime.datetime(2026, 10, 17, 23, 59, 30)))
        self.assertEqual(first, "20261017")

    def test_timezone_aware_input_is_converted_to_utc(self):
        self.assertEqual(render("{today}", batch_ts="2026-10-18T01:30:00+02:00"), "20261017")

    def test_column_dates(self):
        self.assertEqual(
            render(
                "{client_id}_{event_date:%Y%m%d}",
                {"client_id": "acme", "event_date": "2026-10-15"},
                ["client_id", "event_date"],
            ),
            "acme_20261015",
        )
        self.assertEqual(render("{d:%Y%m%d}", {"d": datetime.date(2026, 2, 3)}, ["d"]), "20260203")
        self.assertEqual(render("{d-1d:%Y%m%d}", {"d": datetime.datetime(2026, 3, 1, 5)}, ["d"]), "20260228")

    def test_layout_slash_and_hive(self):
        self.assertEqual(
            render("{client_id}/{today:%Y/%m/%d}", {"client_id": "acme"}, ["client_id"], slash=True), "acme/2026/10/17"
        )
        self.assertEqual(
            render("client={client_id}/dt={today:%Y-%m-%d}", {"client_id": "acme"}, ["client_id"], slash=True),
            "client=acme/dt=2026-10-17",
        )

    def test_seq_and_batch_id(self):
        self.assertEqual(render("{batch_id}-{seq}.csv", seq=3), "b42-00003.csv")
        self.assertEqual(render("{driver_host}_{batch_id}-{seq}.csv", seq=3), "ip-10-0-0-1_b42-00003.csv")


class Safety(unittest.TestCase):
    def test_values_are_sanitized(self):
        self.assertEqual(render("{c}.csv", {"c": "Acme Corp/../x"}, ["c"]), "Acme_Corp_.._x.csv")
        with self.assertRaises(NamingError):
            render("{c}", {"c": ".."}, ["c"])
        with self.assertRaises(NamingError):
            render("{c}", {"c": "   "}, ["c"])

    def test_slash_rules(self):
        with self.assertRaises(NamingError):
            parse_template("a/{today}", [], allow_slash=False)
        with self.assertRaises(NamingError):
            parse_template("{today:%Y/%m}", [], allow_slash=False)
        parse_template("{today:%Y/%m}", [], allow_slash=True)

    def test_a_value_with_slash_cannot_make_a_folder(self):
        self.assertEqual(render("{c}", {"c": "a/b"}, ["c"], slash=True), "a_b")

    def test_parse_errors(self):
        for text, cols in [
            ("{nope}", []),
            ("{today:%Q}", []),
            ("{today:%}", []),
            ("{batch_id:%Y}", []),
            ("{c-1x}", ["c"]),
            ("x y", []),
            ("{today:%Y;rm}", []),
            ("{c}..{c}", ["c"]),
            ("fixed.csv", []),
            ("{today:%-Z}", []),
            ("{}", []),
        ]:
            with self.assertRaises(NamingError, msg=text):
                parse_template(text, cols)

    def test_reserved_split_names(self):
        with self.assertRaises(NamingError):
            parse_template("{today}", ["today"])

    def test_missing_split_value(self):
        with self.assertRaises(NamingError):
            render("{c}", {}, ["c"])


class Planning(unittest.TestCase):
    def test_collision_is_detected_before_any_write(self):
        file_t = parse_template("{c}.csv", ["c"])
        with self.assertRaises(NamingError):
            plan_names(
                file_t,
                None,
                [
                    (("a b",), {"c": "a b"}),
                    (("a/b",), {"c": "a_b"}),
                ],
                BATCH,
                "b1",
            )

    def test_plan(self):
        file_t = parse_template("{client_id}_{today}.csv", ["client_id"])
        layout = parse_template("{client_id}/{today:%Y/%m/%d}", ["client_id"], allow_slash=True)
        plan = plan_names(file_t, layout, [(("a",), {"client_id": "a"}), (("b",), {"client_id": "b"})], BATCH, "b1")
        self.assertEqual(plan[("a",)], "a/2026/10/17/a_20261017.csv")
        self.assertEqual(plan[("b",)], "b/2026/10/17/b_20261017.csv")

    def test_sanitize(self):
        self.assertEqual(sanitize("a b"), "a_b")


if __name__ == "__main__":
    unittest.main()
