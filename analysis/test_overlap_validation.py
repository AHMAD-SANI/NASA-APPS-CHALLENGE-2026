import unittest
from pathlib import Path

from analysis.analyze_data import detect_overlapping_source_product_ranges


class OverlapValidationTests(unittest.TestCase):
    def test_detects_same_product_overlap(self):
        files = [
            Path("MODIS_NRT_2026-10-16_to_2026-10-20_cleaned.csv"),
            Path("MODIS_NRT_2026-10-18_to_2026-10-22_cleaned.csv"),
        ]

        overlap = detect_overlapping_source_product_ranges(files)

        self.assertEqual(len(overlap), 1)
        self.assertEqual(overlap[0]["source_product"], "MODIS_NRT")
        self.assertEqual(overlap[0]["overlap_start"], "2026-10-18")
        self.assertEqual(overlap[0]["overlap_end"], "2026-10-20")

    def test_keeps_different_sensors_separate(self):
        files = [
            Path("MODIS_NRT_2026-10-16_to_2026-10-20_cleaned.csv"),
            Path("VIIRS_SNPP_NRT_2026-10-16_to_2026-10-20_cleaned.csv"),
        ]

        overlap = detect_overlapping_source_product_ranges(files)

        self.assertEqual(overlap, [])


if __name__ == "__main__":
    unittest.main()
