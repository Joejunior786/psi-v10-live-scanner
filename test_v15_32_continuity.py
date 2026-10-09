import tempfile
import unittest
from pathlib import Path
from psi_v15_32_continuity import OpportunityJournal


def structure(stamp):
    return {"symbol":"TESTUSDT","timeframe":"4H","setup":"SUPPORT_RECLAIM",
            "state":"BUY","source_ms":stamp,"entry_low":0.90,
            "entry_high":0.92,"stop":0.86,"tp1":1.05,
            "potential_pct":14.1,"blockers":["NEGATIVE_EXPECTED_VALUE"]}


class JournalTests(unittest.TestCase):
    def test_empty_journal(self):
        book=OpportunityJournal("")
        self.assertEqual(book.stats(100000)["tracked"],0)

    def test_observation_survives_restart_without_buy_authority(self):
        with tempfile.TemporaryDirectory() as root:
            path=str(Path(root)/"journal.json")
            journal=OpportunityJournal(path)
            journal.update(100000,[structure(100000)],[])
            self.assertTrue(journal.view(100001,True)[0]["live_monitoring"])
            self.assertFalse(journal.view(100001,True)[0]["verified_buy_now"])
            self.assertEqual(journal.view(106000,True)[0]["display_state"],
                             "REVALIDATION REQUIRED")
            restored=OpportunityJournal(path).view(100100,True)
            self.assertEqual(restored[0]["display_state"],"REVALIDATION REQUIRED")
            self.assertFalse(restored[0]["uk_spot_account_verified"])

    def test_invalidated_or_stale_cannot_become_buy(self):
        with tempfile.TemporaryDirectory() as root:
            book=OpportunityJournal(str(Path(root)/"journal.json"))
            book.update(100000,[structure(1)],[])
            self.assertEqual(book.stats(100000)["tracked"],0)
            row=structure(110000)
            row["blockers"]=["ENTRY_STRUCTURE_INVALIDATED"]
            book.update(110000,[row],[])
            self.assertEqual(book.view(110100,True)[0]["display_state"],"INVALIDATED")
            self.assertFalse(book.view(110100,True)[0]["verified_buy_now"])

    def test_new_observation_reactivates_only_monitoring(self):
        with tempfile.TemporaryDirectory() as root:
            book=OpportunityJournal(str(Path(root)/"journal.json"))
            book.update(100000,[structure(100000)],[])
            book.update(106000,[structure(106000)],[])
            row=book.view(106001,True)[0]
            self.assertEqual(row["display_state"],"BUY")
            self.assertFalse(row["verified_buy_now"])
