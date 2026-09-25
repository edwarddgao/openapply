import unittest
from unittest import mock

import scripts.build_shortlist as shortlist


class SeniorityRegexTest(unittest.TestCase):
    def test_candidate_bullet_with_have_is_a_requirement(self) -> None:
        # "Have 5+ years" used to be mistaken for company tenure ("we have 20 years").
        [match] = shortlist.year_matches('Have 5+ years of experience building and shipping production software.')
        self.assertTrue(match['valid'])

    def test_company_tenure_is_still_ignored(self) -> None:
        for text in ('For more than 30 years, Verifone has 30 years of experience',
                     'we have 20 years of experience in software, established in Vienna'):
            self.assertFalse(any(m['valid'] for m in shortlist.year_matches(text)), text)

    def test_senior_role_phrase_in_body(self) -> None:
        hits = ('As a Senior Software Engineer on the Hyperscale team',
                "We're looking for Senior and Staff Software Engineers",
                'seeking a Staff Machine Learning Engineer')
        misses = ('You will work alongside senior engineers', 'As a senior member of the team')
        for text in hits:
            self.assertTrue(shortlist.SENIOR_ROLE_PHRASE_RE.search(text), text)
        for text in misses:
            self.assertFalse(shortlist.SENIOR_ROLE_PHRASE_RE.search(text), text)

    def test_university_hire_is_entry(self) -> None:
        self.assertTrue(shortlist.ENTRY_TITLE_RE.search('Software Engineer - University Hire 2027'))

    def test_strip_html_handles_entity_escaped_markup(self) -> None:
        self.assertEqual(shortlist.strip_html('&lt;li&gt;Build &amp;amp; ship&lt;/li&gt;'), 'Build & ship')
        self.assertEqual(shortlist.strip_html('<p>a &amp; b</p>'), 'a & b')


class RoleTitleTest(unittest.TestCase):
    def test_spaced_front_end_needs_an_engineering_noun(self) -> None:
        for title in ('Team Captain Endzone & Loyalty (Front End)', 'Webber - Front End Loader Operator',
                      'Spindle Polishing Technician CWW Front End Nights', 'Full Time - Back End Clerk - Day'):
            self.assertFalse(shortlist.ROLE_RE.search(title), title)
        for title in ('Front End AI Engineer', 'Front-End Developer', 'Senior Back End Developer', 'Backend SWE'):
            self.assertTrue(shortlist.ROLE_RE.search(title), title)

    def test_industrial_engineering_titles(self) -> None:
        for title in ('Electrical Reliability Engineer', 'Pharmaceutical Automation Engineer',
                      'Automation Engineer - Advanced Manufacturing', 'Optical System Integration Engineer',
                      'Flight Systems Engineer', 'Quality & Reliability Engineer', 'Reliability Engineer - Fixed Plant'):
            self.assertTrue(shortlist.INDUSTRIAL_ENGINEERING_TITLE_RE.search(title), title)
        for title in ('Reliability Engineer, Supercomputing', 'Site Reliability Engineer', 'IT Systems Engineer',
                      'Network Automation Engineer', 'HR Systems Integration Engineer', 'Test Automation Engineer',
                      'Design Automation Engineer', 'Integration Engineer'):
            self.assertFalse(shortlist.INDUSTRIAL_ENGINEERING_TITLE_RE.search(title), title)

    def test_business_analyst_titles(self) -> None:
        for title in ('Business Analyst II', 'Technical Business Analyst - HRIS', 'Business Systems Analyst'):
            self.assertTrue(shortlist.BUSINESS_ANALYST_TITLE_RE.search(title), title)
        for title in ('Business Intelligence Analyst', 'Data Analyst'):
            self.assertFalse(shortlist.BUSINESS_ANALYST_TITLE_RE.search(title), title)

    def test_sales_and_product_management_titles(self) -> None:
        for title in ('Outside Sales Engineer', 'Pre-Sales Engineer', 'Solutions Engineer [Pre and Post-Sales]',
                      'Solutions Engineer - Enterprise', 'Solution Architect',
                      'Digital Product Owner', 'Associate Product Manager', 'APM, Growth'):
            self.assertTrue(shortlist.SALES_TITLE_RE.search(title) or shortlist.PRODUCT_MANAGEMENT_TITLE_RE.search(title), title)
        for title in ('Product Engineer', 'Founding Product Engineer', 'Salesforce Developer', 'Software Engineer'):
            self.assertFalse(shortlist.SALES_TITLE_RE.search(title) or shortlist.PRODUCT_MANAGEMENT_TITLE_RE.search(title), title)


class JevScreenTest(unittest.TestCase):
    def row(self, bucket: str, title: str) -> dict:
        return {'seniority_bucket': bucket, 'title': title, 'department': '', '_full_text': title}

    def test_moves_only_confident_senior_ambiguous_rows(self) -> None:
        rows = [self.row('ambiguous-no-years', 'A'), self.row('ambiguous-no-years', 'B'),
                self.row('ambiguous-no-years', 'C'), self.row('explicit-entry-title', 'D')]
        answers = [{'label': 'senior', 'confidence': 0.95, 'probabilities': {}},
                   {'label': 'senior', 'confidence': 0.6, 'probabilities': {}},
                   RuntimeError('boom')]
        with mock.patch('scripts.jev.Jev') as jev:
            jev.return_value.seniority_many.return_value = answers
            moved, stats = shortlist.jev_screen(rows, 0.9, 2)
        self.assertEqual([r['title'] for r in moved], ['A'])
        self.assertEqual(moved[0]['remove_reasons'], 'jev-likely-senior')
        self.assertEqual(rows[1]['jev_confidence'], '0.600')
        self.assertNotIn('jev_seniority', rows[3])
        self.assertEqual(stats, {'mode': 'enforce', 'threshold': 0.9, 'candidates': 3, 'scored': 2,
                                 'failed': 1, 'would_move': 1, 'moved': 1})

    def test_shadow_mode_annotates_without_moving(self) -> None:
        rows = [self.row('ambiguous-no-years', 'A')]
        with mock.patch('scripts.jev.Jev') as jev:
            jev.return_value.seniority_many.return_value = [{'label': 'senior', 'confidence': 0.99, 'probabilities': {}}]
            moved, stats = shortlist.jev_screen(rows, 0.9, 2, enforce=False)
        self.assertEqual(moved, [])
        self.assertEqual((stats['would_move'], stats['moved']), (1, 0))
        self.assertEqual(rows[0]['jev_seniority'], 'senior')
        self.assertNotIn('remove_reasons', rows[0])

    def test_missing_key_keeps_every_row(self) -> None:
        with mock.patch('scripts.jev.Jev', side_effect=RuntimeError('Set OPENROUTER_API_KEY')):
            moved, stats = shortlist.jev_screen([self.row('ambiguous-no-years', 'A')], 0.9, 2)
        self.assertEqual(moved, [])
        self.assertIn('error', stats)


if __name__ == '__main__':
    unittest.main()
