"""Tests for content validation heuristics."""

from municipality_email.content import classify_homepage


class TestClassifyHomepage:
    def test_parked_domain_for_sale(self):
        html = "<html><body><h1>This domain is parked</h1></body></html>"
        assert classify_homepage(html) == ["parked"]

    def test_parked_buy_domain(self):
        html = "<html><body>Buy this domain now!</body></html>"
        assert classify_homepage(html) == ["parked"]

    def test_parked_coming_soon(self):
        html = "<html><body>Coming soon</body></html>"
        assert classify_homepage(html) == ["parked"]

    def test_parked_sedo(self):
        html = "<html><body>sedo.com marketplace</body></html>"
        assert classify_homepage(html) == ["parked"]

    def test_parked_german(self):
        html = "<html><body>Diese Domain steht zum Verkauf</body></html>"
        assert classify_homepage(html) == ["parked"]

    def test_municipality_keywords_de(self):
        html = "<html><body><h1>Gemeinde Musterstadt</h1><p>Rathaus</p></body></html>"
        assert classify_homepage(html) == ["has_municipality_keywords"]

    def test_municipality_keywords_fr(self):
        html = "<html><body><h1>Commune de Genève</h1></body></html>"
        assert classify_homepage(html) == ["has_municipality_keywords"]

    def test_municipality_keywords_it(self):
        html = "<html><body><h1>Comune di Lugano</h1></body></html>"
        assert classify_homepage(html) == ["has_municipality_keywords"]

    def test_no_keywords(self):
        html = "<html><body><h1>Welcome to our website</h1></body></html>"
        assert classify_homepage(html) == ["no_municipality_keywords"]

    def test_empty_html(self):
        assert classify_homepage("") == ["no_municipality_keywords"]

    def test_parked_plus_municipal_keywords_not_parked(self):
        """If both parked indicators and municipality keywords are present,
        treat as legitimate (municipality keywords override parked heuristic)."""
        html = (
            "<html><body>"
            "<h1>Gemeinde Musterstadt</h1>"
            "<footer>Hosted by GoDaddy</footer>"
            "</body></html>"
        )
        assert classify_homepage(html) == ["has_municipality_keywords"]

    def test_case_insensitive(self):
        html = "<html><body><h1>STADTVERWALTUNG</h1></body></html>"
        assert classify_homepage(html) == ["has_municipality_keywords"]

    def test_parked_case_insensitive(self):
        html = "<html><body>UNDER CONSTRUCTION</body></html>"
        assert classify_homepage(html) == ["parked"]
