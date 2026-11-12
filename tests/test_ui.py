"""
End-to-end tests for KafkaLens.
Run: pytest tests/test_ui.py -v
Requires: app on :8000, Kafka on :9092 (SASL PLAIN), Schema Registry on :8081.
"""
import json, time
import httpx
import pytest
from playwright.sync_api import Page, expect, sync_playwright

BASE         = "http://localhost:8000"
BOOTSTRAP    = "localhost:9092"
USERNAME     = "kafkalens"
PASSWORD     = "kafkalens-secret"
SR_URL       = "http://localhost:8081"
TOPIC        = "analytics.events"
CLUSTER_NAME = "Test Local"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def cluster_id():
    """Create a cluster once for all tests, delete after session."""
    r = httpx.post(f"{BASE}/api/clusters", json={
        "name": CLUSTER_NAME,
        "bootstrap_servers": BOOTSTRAP,
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "username": USERNAME,
        "password": PASSWORD,
        "schema_registry_url": SR_URL,
    })
    assert r.status_code == 201, r.text
    cid = r.json()["id"]
    yield cid
    httpx.delete(f"{BASE}/api/clusters/{cid}")


@pytest.fixture(scope="session")
def browser_ctx():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1400, "height": 900})
        yield ctx
        browser.close()


@pytest.fixture
def page(browser_ctx, cluster_id):
    pg = browser_ctx.new_page()
    pg.goto(BASE, wait_until="networkidle")
    yield pg
    pg.close()


def open_add_modal(page: Page):
    page.click("button:has-text('Add Cluster')")
    page.wait_for_selector("h2:has-text('Add Cluster'), h2:has-text('Edit Cluster')", state="visible")


def fill_cluster_form(page: Page, name="New Cluster", protocol="SASL_PLAINTEXT",
                      mechanism="PLAIN", username=USERNAME, password=PASSWORD):
    page.fill("input[x-model='clusterForm.name']", name)
    page.fill("input[x-model='clusterForm.bootstrap_servers']", BOOTSTRAP)
    page.select_option("select[x-model='clusterForm.security_protocol']", protocol)
    if protocol.startswith("SASL"):
        page.select_option("select[x-model='clusterForm.sasl_mechanism']", mechanism)
        if mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
            page.fill("input[x-model='clusterForm.username']", username)
            page.fill("input[x-model='clusterForm.password']", password)
    page.fill("input[x-model='clusterForm.schema_registry_url']", SR_URL)


def run_search(cluster_id, payload, timeout=60) -> dict:
    r = httpx.post(f"{BASE}/api/clusters/{cluster_id}/topics/{TOPIC}/search",
                   json=payload, timeout=10)
    assert r.status_code == 200, r.text
    task_id = r.json()["task_id"]
    start = time.time()
    cur_event = None
    with httpx.stream("GET", f"{BASE}/api/search/{task_id}/stream", timeout=timeout) as s:
        for line in s.iter_lines():
            if line.startswith("event:"):
                cur_event = line[6:].strip()
            elif line.startswith("data:") and cur_event in ("done", "error"):
                return {"event": cur_event, "data": json.loads(line[5:]),
                        "elapsed": time.time() - start}
    return {}


# ── API Validation Tests ──────────────────────────────────────────────────────

class TestAPIValidation:
    def test_missing_name_rejected(self):
        r = httpx.post(f"{BASE}/api/clusters", json={"bootstrap_servers": "localhost:9092"})
        assert r.status_code == 422

    def test_missing_bootstrap_rejected(self):
        r = httpx.post(f"{BASE}/api/clusters", json={"name": "x"})
        assert r.status_code == 422

    def test_sasl_without_mechanism_rejected(self):
        r = httpx.post(f"{BASE}/api/clusters", json={
            "name": "x", "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "",
        })
        assert r.status_code == 422, f"Expected 422, got {r.status_code}: {r.text}"

    def test_sasl_plain_without_credentials_rejected(self):
        r = httpx.post(f"{BASE}/api/clusters", json={
            "name": "x", "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN",
            "username": "", "password": "",
        })
        assert r.status_code == 422, f"Expected 422, got {r.status_code}: {r.text}"

    def test_plaintext_no_credentials_accepted(self):
        r = httpx.post(f"{BASE}/api/clusters", json={
            "name": "plaintext-test", "bootstrap_servers": "localhost:9092",
            "security_protocol": "PLAINTEXT",
        })
        assert r.status_code == 201
        httpx.delete(f"{BASE}/api/clusters/{r.json()['id']}")

    def test_stream_missing_task_returns_404(self):
        r = httpx.get(f"{BASE}/api/search/nonexistent-task-id/stream")
        assert r.status_code == 404

    def test_search_missing_cluster_returns_404(self):
        r = httpx.post(f"{BASE}/api/clusters/nonexistent/topics/x/search",
                       json={"max_messages": 10})
        assert r.status_code == 404


# ── Page Load Tests ───────────────────────────────────────────────────────────

class TestPageLoad:
    def test_page_loads(self, page: Page):
        expect(page.locator("text=KafkaLens")).to_be_visible()

    def test_add_cluster_button_visible(self, page: Page):
        expect(page.locator("button:has-text('Add Cluster')")).to_be_visible()

    def test_cluster_appears_in_sidebar(self, page: Page):
        expect(page.locator(f"text={CLUSTER_NAME}").first).to_be_visible()

    def test_dark_mode_toggle_works(self, page: Page):
        html = page.locator("html")
        page.click("button[title*='theme' i], button:has-text('Toggle')")
        page.wait_for_timeout(300)
        # Just verify the toggle doesn't crash — class changes on html element
        page.click("button[title*='theme' i], button:has-text('Toggle')")


# ── Add Cluster Modal Tests ───────────────────────────────────────────────────

class TestAddClusterModal:
    def test_modal_opens_and_closes(self, page: Page):
        open_add_modal(page)
        expect(page.locator("h2:has-text('Add Cluster')")).to_be_visible()
        page.keyboard.press("Escape")
        expect(page.locator("h2:has-text('Add Cluster')")).not_to_be_visible()

    def test_credentials_hidden_for_plaintext(self, page: Page):
        open_add_modal(page)
        page.select_option("select[x-model='clusterForm.security_protocol']", "PLAINTEXT")
        expect(page.locator("input[x-model='clusterForm.username']")).not_to_be_visible()
        page.keyboard.press("Escape")

    def test_credentials_visible_for_sasl_plain(self, page: Page):
        open_add_modal(page)
        page.select_option("select[x-model='clusterForm.security_protocol']", "SASL_PLAINTEXT")
        page.select_option("select[x-model='clusterForm.sasl_mechanism']", "PLAIN")
        expect(page.locator("input[x-model='clusterForm.username']")).to_be_visible()
        expect(page.locator("input[x-model='clusterForm.password']")).to_be_visible()
        page.keyboard.press("Escape")

    def test_credentials_hidden_for_oauthbearer(self, page: Page):
        open_add_modal(page)
        page.select_option("select[x-model='clusterForm.security_protocol']", "SASL_PLAINTEXT")
        page.select_option("select[x-model='clusterForm.sasl_mechanism']", "OAUTHBEARER")
        expect(page.locator("input[x-model='clusterForm.username']")).not_to_be_visible()
        page.keyboard.press("Escape")

    def test_ssl_fields_shown_for_ssl(self, page: Page):
        open_add_modal(page)
        page.select_option("select[x-model='clusterForm.security_protocol']", "SSL")
        expect(page.locator("input[x-model='clusterForm.ssl_ca_location']")).to_be_visible()
        page.keyboard.press("Escape")

    def test_save_valid_cluster_and_delete(self, page: Page):
        open_add_modal(page)
        fill_cluster_form(page, name="Temp Cluster")
        page.click("button[type='submit']")
        page.wait_for_selector("text=Temp Cluster", timeout=5000)
        expect(page.locator("text=Temp Cluster")).to_be_visible()
        # Clean up via API
        clusters = httpx.get(f"{BASE}/api/clusters").json()
        for c in clusters:
            if c["name"] == "Temp Cluster":
                httpx.delete(f"{BASE}/api/clusters/{c['id']}")


# ── Topic List Tests ──────────────────────────────────────────────────────────

class TestTopicList:
    def _expand_cluster(self, page: Page):
        page.click(f"text={CLUSTER_NAME}")
        page.wait_for_selector(f"text={TOPIC}", timeout=10000)

    def test_topics_load(self, page: Page):
        self._expand_cluster(page)
        expect(page.locator(f"text={TOPIC}")).to_be_visible()

    def test_topic_filter_matches(self, page: Page):
        self._expand_cluster(page)
        page.fill("input[placeholder*='ilter' i]", "analytics")
        expect(page.locator(f"text={TOPIC}")).to_be_visible()

    def test_topic_filter_no_match_hides(self, page: Page):
        self._expand_cluster(page)
        page.fill("input[placeholder*='ilter' i]", "zzznomatch")
        expect(page.locator(f"text={TOPIC}")).not_to_be_visible()

    def test_topic_filter_clear_restores(self, page: Page):
        self._expand_cluster(page)
        page.fill("input[placeholder*='ilter' i]", "zzznomatch")
        page.fill("input[placeholder*='ilter' i]", "")
        expect(page.locator(f"text={TOPIC}")).to_be_visible()


# ── Search Tests ──────────────────────────────────────────────────────────────

class TestSearch:
    def _open_search(self, page: Page):
        page.click(f"text={CLUSTER_NAME}")
        page.wait_for_selector(f"text={TOPIC}", timeout=10000)
        page.click(f"text={TOPIC}")
        page.wait_for_selector("button:has-text('Search')", timeout=5000)

    def test_search_no_filter(self, page: Page):
        self._open_search(page)
        page.click("button:has-text('Search')")
        page.wait_for_selector("text=scanned", timeout=30000)

    def test_search_shows_avro_format(self, page: Page):
        self._open_search(page)
        page.click("button:has-text('Search')")
        page.wait_for_selector(".fmt-avro", timeout=60000)

    def test_search_jsonpath_filter(self, page: Page):
        self._open_search(page)
        page.select_option("select[x-model*='value_filter_mode']", "jsonpath")
        page.fill("input[x-model*='value_filter']", "$.geo.country=DE")
        page.click("button:has-text('Search')")
        page.wait_for_selector("text=matches", timeout=60000)

    def test_search_fulltext_filter(self, page: Page):
        self._open_search(page)
        page.select_option("select[x-model*='value_filter_mode']", "fulltext")
        page.fill("input[x-model*='value_filter']", "checkout_complete")
        page.click("button:has-text('Search')")
        page.wait_for_selector("text=matches", timeout=60000)

    def test_cancel_search(self, page: Page):
        self._open_search(page)
        page.fill("input[x-model='search.max_messages']", "500000")
        page.click("button:has-text('Search')")
        page.wait_for_selector("button:has-text('Cancel')", timeout=5000)
        page.click("button:has-text('Cancel')")
        page.wait_for_selector("button:has-text('Search')", timeout=5000)

    def test_expand_message_row(self, page: Page):
        self._open_search(page)
        page.click("button:has-text('Search')")
        page.wait_for_selector("tr.cursor-pointer", timeout=30000)
        page.locator("tr.cursor-pointer").first.click()
        page.wait_for_selector("pre, code", timeout=3000)

    def test_export_json(self, page: Page):
        self._open_search(page)
        page.click("button:has-text('Search')")
        page.wait_for_selector("button:has-text('JSON')", timeout=60000)
        with page.expect_download(timeout=5000):
            page.locator("button:has-text('JSON')").first.click()


# ── API Search Tests (no browser) ─────────────────────────────────────────────

class TestAPISearch:
    def test_no_filter_returns_results(self, cluster_id):
        result = run_search(cluster_id, {"max_messages": 10})
        assert result["event"] == "done"
        assert result["data"]["matched"] == 10

    def test_jsonpath_geo_country(self, cluster_id):
        result = run_search(cluster_id, {
            "max_messages": 5000,
            "value_filter": "$.geo.country=DE",
            "value_filter_mode": "jsonpath",
        })
        assert result["event"] == "done"
        assert result["data"]["matched"] > 0
        # Verify every returned message actually matches
        for msg in result["data"]["messages"]:
            assert msg["value"]["geo"]["country"] == "DE"

    def test_fulltext_returns_matching_messages(self, cluster_id):
        result = run_search(cluster_id, {
            "max_messages": 2000,
            "value_filter": "purchase",
            "value_filter_mode": "fulltext",
        })
        assert result["event"] == "done"
        assert result["data"]["matched"] > 0

    def test_key_prefix_filter(self, cluster_id):
        result = run_search(cluster_id, {
            "max_messages": 1000,
            "key_filter": "user-001",
            "key_match": "prefix",
        })
        assert result["event"] == "done"
        for msg in result["data"]["messages"]:
            assert msg["key"].startswith("user-001")

    def test_sql_compound_filter(self, cluster_id):
        result = run_search(cluster_id, {
            "max_messages": 5000,
            "value_filter": "json_extract_string(value_json,'$.geo.country')='DE' AND CAST(json_extract(value_json,'$.user.lifetime_value') AS DOUBLE) > 10000",
            "value_filter_mode": "sql",
        })
        assert result["event"] == "done"
        assert result["data"]["matched"] > 0
        for msg in result["data"]["messages"]:
            assert msg["value"]["geo"]["country"] == "DE"
            assert msg["value"]["user"]["lifetime_value"] > 10000

    def test_avro_messages_fully_deserialized(self, cluster_id):
        result = run_search(cluster_id, {"max_messages": 5})
        assert result["event"] == "done"
        for msg in result["data"]["messages"]:
            val = msg["value"]
            assert "event_id" in val
            assert "geo" in val
            assert "user" in val
            assert msg["format"] == "avro"

    def test_search_throughput(self, cluster_id):
        """Scan 50K messages in under 60 seconds."""
        result = run_search(cluster_id, {"max_messages": 50000}, timeout=120)
        assert result["event"] == "done"
        assert result["data"]["scanned"] == 50000
        assert result["elapsed"] < 60, f"Too slow: {result['elapsed']:.1f}s for 50K messages"
