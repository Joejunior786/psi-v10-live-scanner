import unittest
from psi_v15_33_integrity import DepthGap, SyncedDepthBook

def pkt(u, U=None, b=None, a=None):
    return {"U": u if U is None else U, "u": u,
            "b": b if b is not None else [], "a": a if a is not None else []}

def seed(last=100):
    return {"lastUpdateId": last, "bids": [["10","4"],["9","3"]],
            "asks": [["11","2"],["12","3"]]}

class SyncedBookTests(unittest.TestCase):
    def test_requires_exchange_snapshot_and_sequence_bridge(self):
        book=SyncedDepthBook()
        self.assertIsNone(book.receive(pkt(101,b=[["10","6"]]),2000))
        self.assertFalse(book.synced)
        samples=book.seed(seed())
        self.assertTrue(book.synced)
        self.assertEqual(len(samples),1)
        self.assertEqual(samples[0]["bids"][0],(10.0,6.0))
        self.assertEqual(samples[0]["_received_ms"],2000)

    def test_reject_snapshot_older_than_first_websocket_event(self):
        book=SyncedDepthBook()
        book.receive(pkt(200,199),1000)
        self.assertEqual(book.seed(seed(100)),[])
        self.assertFalse(book.synced)
        self.assertTrue(book.seed(seed(199)))

    def test_gap_invalidates_and_must_resnapshot(self):
        book=SyncedDepthBook()
        book.receive(pkt(101),1000)
        book.seed(seed())
        with self.assertRaises(DepthGap):
            book.receive(pkt(105,105),2000)
        self.assertFalse(book.synced)
        self.assertEqual(book.gaps,1)
        self.assertEqual(book.seed(seed(104))[0]["lastUpdateId"],105)

    def test_zero_quantity_deletes_and_empty_packet_is_valid(self):
        book=SyncedDepthBook()
        book.receive(pkt(101),1000)
        book.seed(seed())
        x=book.receive(pkt(102,b=[["10","0"]]),1100)
        self.assertEqual(x["bids"][0],(9.0,3.0))
        x=book.receive(pkt(103),1200)
        self.assertEqual(x["lastUpdateId"],103)

    def test_overlapping_update_and_stale_replay(self):
        book=SyncedDepthBook()
        book.receive(pkt(105,100),1000)
        self.assertEqual(book.seed(seed(102))[0]["lastUpdateId"],105)
        self.assertIsNone(book.receive(pkt(104),1100))
        self.assertEqual(book.receive(pkt(107,106),1200)["lastUpdateId"],107)

    def test_crossed_book_fails_closed(self):
        book=SyncedDepthBook()
        book.receive(pkt(101),1000)
        book.seed(seed())
        with self.assertRaises(DepthGap):
            book.receive(pkt(102,b=[["13","1"]]),1100)
        self.assertFalse(book.synced)

if __name__ == "__main__":
    unittest.main()
