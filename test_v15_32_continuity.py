import unittest
from psi_v15_32_continuity import OpportunityJournal

class JournalTests(unittest.TestCase):
    def test_empty_journal(self):
        book = OpportunityJournal('')
        self.assertEqual(book.stats(100000)['tracked'], 0)
