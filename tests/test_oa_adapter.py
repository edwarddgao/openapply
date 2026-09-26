import io
import unittest
from unittest import mock

import oa_adapter

FEED = b"""<?xml version="1.0" encoding="utf-8"?>
<source>
  <publisher>Workable</publisher>
  <job>
    <title><![CDATA[Software Engineer]]></title>
    <date><![CDATA[Mon, 21 Sep 2026 14:05:00 UTC]]></date>
    <referencenumber><![CDATA[ABC123]]></referencenumber>
    <url><![CDATA[https://apply.workable.com/j/ABC123]]></url>
    <company><![CDATA[Acme Robotics, Inc.]]></company>
    <city><![CDATA[New York]]></city>
    <state><![CDATA[New York]]></state>
    <country><![CDATA[US]]></country>
    <remote><![CDATA[false]]></remote>
    <description><![CDATA[<p>Build things.</p>]]></description>
    <jobtype><![CDATA[Full-time]]></jobtype>
    <category><![CDATA[Engineering]]></category>
  </job>
  <job>
    <title><![CDATA[Software Engineer]]></title>
    <date><![CDATA[Mon, 21 Sep 2026 14:05:00 UTC]]></date>
    <referencenumber><![CDATA[ABC123]]></referencenumber>
    <url><![CDATA[https://apply.workable.com/j/ABC123]]></url>
    <company><![CDATA[Acme Robotics, Inc.]]></company>
    <city><![CDATA[Toronto]]></city><state><![CDATA[Ontario]]></state><country><![CDATA[CA]]></country>
    <remote><![CDATA[false]]></remote>
    <description><![CDATA[<p>Build things.</p>]]></description>
  </job>
  <job>
    <title><![CDATA[Data Engineer]]></title>
    <date><![CDATA[not a date]]></date>
    <referencenumber><![CDATA[XYZ]]></referencenumber>
    <url><![CDATA[]]></url>
    <company><![CDATA[Beta]]></company>
    <city><![CDATA[]]></city><state><![CDATA[]]></state><country><![CDATA[CA]]></country>
    <remote><![CDATA[true]]></remote>
    <description><![CDATA[]]></description>
  </job>
</source>
"""


class WorkableFeedTest(unittest.TestCase):
    @mock.patch('oa_adapter.urllib.request.urlopen')
    def test_feed_jobs_map_to_postings(self, urlopen: mock.Mock) -> None:
        urlopen.return_value.__enter__.return_value = io.BytesIO(FEED)
        first, second = oa_adapter.fetch_workable()
        self.assertEqual(first.id, 'workable:acme-robotics-inc:ABC123')
        self.assertEqual(first.source_slug, 'acme-robotics-inc')
        self.assertEqual(first.apply_url, 'https://apply.workable.com/j/ABC123')
        self.assertEqual(first.locations, ['New York, New York, United States', 'Toronto, Ontario, Canada'])  # merged duplicate
        self.assertIs(first.remote, False)
        self.assertEqual(first.posted_at, '2026-09-21T14:05:00Z')
        self.assertEqual(first.description_html, '<p>Build things.</p>')
        self.assertEqual((first.employment_type, first.department), ('Full-time', 'Engineering'))
        self.assertEqual(second.apply_url, 'https://apply.workable.com/j/XYZ')
        self.assertEqual(second.locations, ['Canada'])
        self.assertIs(second.remote, True)
        self.assertIsNone(second.posted_at)
        self.assertIsNone(second.description_html)

    def test_country_codes_become_names_so_they_cannot_read_as_states(self) -> None:
        self.assertEqual(oa_adapter._country_name('CO'), 'Colombia')
        self.assertEqual(oa_adapter._country_name('ID'), 'Indonesia')
        self.assertEqual(oa_adapter._country_name('US'), 'United States')
        self.assertEqual(oa_adapter._country_name(''), '')


if __name__ == '__main__':
    unittest.main()
