from unittest import mock

from django.test import TestCase
from rest_framework.test import APIClient

from fires import archive_prepare


class NameCandidateTests(TestCase):
    def test_iso_names_map_to_firms_spellings(self):
        self.assertIn('Cote_d_Ivoire', archive_prepare.name_candidates("Côte d'Ivoire"))
        self.assertIn('Democratic_Republic_of_the_Congo', archive_prepare.name_candidates('Congo, The Democratic Republic of the'))
        self.assertIn('Republic_of_Korea', archive_prepare.name_candidates('Korea, Republic of'))
        self.assertEqual(archive_prepare.name_candidates('Viet Nam')[0], 'Vietnam')


class ArchiveEndpointTests(TestCase):
    def test_requires_country_code(self):
        self.assertEqual(APIClient().get('/api/harmonized/archive/').status_code, 400)

    def test_post_starts_preparation_for_known_country(self):
        countries = {'results': [{'country': 'Ghana', 'country_code': 'GHA', 'country_iso2': 'GH'}]}
        with mock.patch('fires.views.list_countries', return_value=countries), \
             mock.patch.object(archive_prepare, 'start', return_value={'status': 'queued', 'progress': 0}) as start:
            response = APIClient().post('/api/harmonized/archive/', {'country_code': 'gha'}, format='json')
        self.assertEqual(response.status_code, 202)
        start.assert_called_once_with('GHA', 'Ghana')
        self.assertEqual(response.data['status'], 'queued')
