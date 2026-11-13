// KafkaLens — Alpine.js app logic

function kafkaApp(initialClusters) {
  return {
    clusters: initialClusters || [],
    activeClusters: {},       // clusterId -> {expanded, topics, loading}
    selectedCluster: null,
    selectedTopic: null,
    topicInfo: null,
    topicLoading: false,

    // Search state
    search: {
      from_timestamp: '',
      to_timestamp: '',
      from_offset: '',
      to_offset: '',
      key_filter: '',
      key_match: 'contains',
      value_filter: '',
      value_filter_mode: 'fulltext',
      format: 'auto',
      max_messages: 1000,
    },
    searching: false,
    taskId: null,
    scanned: 0,
    matched: 0,
    messages: [],
    searchError: null,
    sseSource: null,
    searchStartTime: null,
    searchElapsed: null,
    _elapsedTimer: null,
    page: 1,
    pageSize: 50,

    // Modal state
    showAddModal: false,
    editingCluster: null,
    clusterForm: defaultClusterForm(),
    formError: null,
    testResult: null,
    testLoading: false,

    // Message detail
    expandedRow: null,
    topicFilter: '',

    async init() {
      // already have clusters from server
    },

    // ---- Cluster tree ----
    async toggleCluster(cluster) {
      const id = cluster.id;
      if (!this.activeClusters[id]) {
        this.activeClusters[id] = { expanded: false, topics: [], loading: false, error: null };
      }
      const state = this.activeClusters[id];
      state.expanded = !state.expanded;
      if (state.expanded && state.topics.length === 0) {
        await this.loadTopics(cluster);
      }
    },

    async loadTopics(cluster) {
      const state = this.activeClusters[cluster.id];
      state.loading = true;
      state.error = null;
      try {
        const resp = await fetch(`/api/clusters/${cluster.id}/topics`);
        if (!resp.ok) {
          const body = await resp.json().catch(() => ({ detail: resp.statusText }));
          throw new Error(body.detail || resp.statusText);
        }
        state.topics = await resp.json();
      } catch (e) {
        state.topics = [];
        state.error = e.message;
      } finally {
        state.loading = false;
      }
    },

    filteredTopics(clusterId) {
      const state = this.activeClusters[clusterId];
      if (!state) return [];
      if (!this.topicFilter) return state.topics;
      const f = this.topicFilter.toLowerCase();
      return state.topics.filter(t => t.name.toLowerCase().includes(f));
    },

    async selectTopic(cluster, topic) {
      this.selectedCluster = cluster;
      this.selectedTopic = topic;
      this.topicInfo = null;
      this.messages = [];
      this.scanned = 0;
      this.matched = 0;
      this.searchError = null;
      this.expandedRow = null;
      this.topicLoading = true;

      try {
        const resp = await fetch(`/api/clusters/${cluster.id}/topics/${topic.name}/info`);
        if (resp.ok) {
          this.topicInfo = await resp.json();
        }
      } catch (e) {}
      this.topicLoading = false;
    },

    // ---- Search ----
    async startSearch() {
      if (!this.selectedCluster || !this.selectedTopic) return;
      if (this.sseSource) {
        this.sseSource.close();
        if (this.taskId) {
          fetch(`/api/search/${this.taskId}`, { method: 'DELETE' }).catch(() => {});
        }
      }
      this.searching = true;
      this.messages = [];
      this.scanned = 0;
      this.matched = 0;
      this.searchError = null;
      this.expandedRow = null;
      this.page = 1;
      this.searchStartTime = Date.now();
      this.searchElapsed = null;
      if (this._elapsedTimer) clearInterval(this._elapsedTimer);
      this._elapsedTimer = setInterval(() => {
        const ms = Date.now() - this.searchStartTime;
        const s = (ms / 1000).toFixed(1);
        this.searchElapsed = s + 's';
      }, 100);

      const body = this._buildSearchBody();
      let taskId;
      try {
        const resp = await fetch(
          `/api/clusters/${this.selectedCluster.id}/topics/${this.selectedTopic.name}/search`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
        );
        if (!resp.ok) {
          this.searchError = await resp.text();
          this.searching = false;
          return;
        }
        const data = await resp.json();
        taskId = data.task_id;
        this.taskId = taskId;
      } catch (e) {
        this.searchError = e.message;
        this.searching = false;
        return;
      }

      const es = new EventSource(`/api/search/${taskId}/stream`);
      this.sseSource = es;

      es.addEventListener('progress', (e) => {
        const d = JSON.parse(e.data);
        this.scanned = d.scanned;
        this.matched = d.matched;
      });

      es.addEventListener('results', (e) => {
        this.messages = JSON.parse(e.data);
        this.page = 1;
      });

      es.addEventListener('done', (e) => {
        const d = JSON.parse(e.data);
        this.scanned = d.scanned;
        this.matched = d.matched;
        this.messages = d.messages || [];
        this.page = 1;
        this.searching = false;
        clearInterval(this._elapsedTimer);
        const ms = Date.now() - this.searchStartTime;
        this.searchElapsed = (ms / 1000).toFixed(1) + 's';
        es.close();
      });

      es.addEventListener('error', (e) => {
        try {
          const d = JSON.parse(e.data);
          this.searchError = d.error;
        } catch {}
        this.searching = false;
        clearInterval(this._elapsedTimer);
        es.close();
      });

      es.onerror = () => {
        if (this.searching) {
          this.searching = false;
          clearInterval(this._elapsedTimer);
        }
        es.close();
      };
    },

    cancelSearch() {
      if (this.sseSource) this.sseSource.close();
      if (this.taskId) {
        fetch(`/api/search/${this.taskId}`, { method: 'DELETE' }).catch(() => {});
      }
      this.searching = false;
    },

    _buildSearchBody() {
      const s = this.search;
      const body = {
        format: s.format,
        max_messages: parseInt(s.max_messages) || 1000,
        key_match: s.key_match,
        value_filter_mode: s.value_filter_mode,
      };
      if (s.key_filter) body.key_filter = s.key_filter;
      if (s.value_filter) body.value_filter = s.value_filter;
      if (s.from_timestamp) body.from_timestamp = new Date(s.from_timestamp).toISOString();
      if (s.to_timestamp) body.to_timestamp = new Date(s.to_timestamp).toISOString();
      if (s.from_offset !== '') body.from_offset = parseInt(s.from_offset);
      if (s.to_offset !== '') body.to_offset = parseInt(s.to_offset);
      return body;
    },

    // ---- Progress ----
    progressPercent() {
      if (!this.topicInfo || !this.topicInfo.total_messages) return 0;
      return Math.min(100, Math.round((this.scanned / this.topicInfo.total_messages) * 100));
    },

    // ---- Message detail ----
    toggleRow(idx) {
      this.expandedRow = this.expandedRow === idx ? null : idx;
      if (this.expandedRow === idx) {
        this.$nextTick(() => {
          document.querySelectorAll('.json-viewer code').forEach(el => hljs.highlightElement(el));
        });
      }
    },

    valuePreview(msg) {
      if (msg.format === 'error') {
        const hint = msg.value?._hint || msg.value?._error || 'Decode error';
        return `⚠ ${hint}`;
      }
      const v = msg.value_raw || '';
      if (v && v.charCodeAt(0) === 0) return '[binary data — check schema registry]';
      return v.length > 80 ? v.slice(0, 80) + '…' : v;
    },

    formattedValue(msg) {
      if (msg.format === 'error') {
        return JSON.stringify(msg.value, null, 2);
      }
      if (typeof msg.value === 'object' && msg.value !== null) {
        return JSON.stringify(msg.value, null, 2);
      }
      return msg.value_raw || '';
    },

    copyToClipboard(text) {
      navigator.clipboard.writeText(text).catch(() => {});
    },

    // ---- Export ----
    exportJson() {
      const blob = new Blob([JSON.stringify(this.messages, null, 2)], { type: 'application/json' });
      this._download(blob, 'messages.json');
    },

    exportCsv() {
      const cols = ['partition', 'offset', 'timestamp_human', 'key', 'value_raw'];
      const rows = this.messages.map(m => cols.map(c => JSON.stringify(m[c] ?? '')).join(','));
      const csv = [cols.join(','), ...rows].join('\n');
      const blob = new Blob([csv], { type: 'text/csv' });
      this._download(blob, 'messages.csv');
    },

    _download(blob, filename) {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = filename; a.click();
      URL.revokeObjectURL(url);
    },

    // ---- Cluster modal ----
    openAddModal() {
      this.clusterForm = defaultClusterForm();
      this.editingCluster = null;
      this.formError = null;
      this.testResult = null;
      this.showAddModal = true;
    },

    openEditModal(cluster) {
      this.clusterForm = {
        name: cluster.name,
        bootstrap_servers: cluster.bootstrap_servers,
        security_protocol: cluster.security_protocol,
        sasl_mechanism: cluster.sasl_mechanism || '',
        username: '',
        password: '',
        schema_registry_url: cluster.schema_registry_url || '',
        schema_registry_username: '',
        schema_registry_password: '',
        ssl_ca_location: '',
        ssl_cert_location: '',
        ssl_key_location: '',
        ssl_key_password: '',
        tls_skip_verify: cluster.tls_skip_verify || false,
      };
      this.editingCluster = cluster;
      this.formError = null;
      this.testResult = null;
      this.showAddModal = true;
    },

    _normalizeUrl(url) {
      if (url && !url.includes('://')) return 'http://' + url;
      return url;
    },

    async saveCluster() {
      this.formError = null;
      const f = this.clusterForm;
      const payload = {
        name: f.name,
        bootstrap_servers: f.bootstrap_servers,
        security_protocol: f.security_protocol,
        sasl_mechanism: f.sasl_mechanism,
        username: f.username || null,
        password: f.password || null,
        schema_registry_url: this._normalizeUrl(f.schema_registry_url) || null,
        schema_registry_username: f.schema_registry_username || null,
        schema_registry_password: f.schema_registry_password || null,
      };

      if (f.ssl_ca_location || f.ssl_cert_location || f.ssl_key_location) {
        payload.ssl = {
          ca_location: f.ssl_ca_location || null,
          certificate_location: f.ssl_cert_location || null,
          key_location: f.ssl_key_location || null,
          key_password: f.ssl_key_password || null,
        };
      }
      payload.tls_skip_verify = f.tls_skip_verify || false;

      try {
        let resp;
        if (this.editingCluster) {
          resp = await fetch(`/api/clusters/${this.editingCluster.id}`, {
            method: 'PUT', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
        } else {
          resp = await fetch('/api/clusters', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
        }
        if (!resp.ok) {
          this.formError = await resp.text();
          return;
        }
        const saved = await resp.json();
        if (this.editingCluster) {
          const idx = this.clusters.findIndex(c => c.id === saved.id);
          if (idx >= 0) this.clusters[idx] = saved;
        } else {
          this.clusters.push(saved);
        }
        this.showAddModal = false;
      } catch (e) {
        this.formError = e.message;
      }
    },

    async testClusterConnection() {
      if (!this.editingCluster) {
        this.testResult = { ok: false, error: 'Save the cluster first to test.' };
        return;
      }
      this.testLoading = true;
      this.testResult = null;
      try {
        const resp = await fetch(`/api/clusters/${this.editingCluster.id}/test`, { method: 'POST' });
        this.testResult = await resp.json();
      } catch (e) {
        this.testResult = { ok: false, error: e.message };
      } finally {
        this.testLoading = false;
      }
    },

    async deleteCluster(cluster) {
      if (!confirm(`Delete cluster "${cluster.name}"?`)) return;
      await fetch(`/api/clusters/${cluster.id}`, { method: 'DELETE' });
      this.clusters = this.clusters.filter(c => c.id !== cluster.id);
      delete this.activeClusters[cluster.id];
      if (this.selectedCluster?.id === cluster.id) {
        this.selectedCluster = null;
        this.selectedTopic = null;
        this.topicInfo = null;
        this.messages = [];
      }
    },

    // ---- Pagination ----
    pagedMessages() {
      const start = (this.page - 1) * this.pageSize;
      return this.messages.slice(start, start + this.pageSize);
    },

    totalPages() {
      return Math.max(1, Math.ceil(this.messages.length / this.pageSize));
    },

    prevPage() {
      if (this.page > 1) { this.page--; this.expandedRow = null; }
    },

    nextPage() {
      if (this.page < this.totalPages()) { this.page++; this.expandedRow = null; }
    },

    gotoPage(n) {
      const p = parseInt(n);
      if (!isNaN(p) && p >= 1 && p <= this.totalPages()) {
        this.page = p;
        this.expandedRow = null;
      }
    },

    protoBadgeClass(proto) {
      const map = { PLAINTEXT: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
                    SSL: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
                    SASL_PLAINTEXT: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200',
                    SASL_SSL: 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200' };
      return map[proto] || 'bg-gray-100 text-gray-700';
    },

    fmtClass(fmt) {
      return `fmt-${fmt}`;
    },
  };
}

function defaultClusterForm() {
  return {
    name: '',
    bootstrap_servers: '',
    security_protocol: 'PLAINTEXT',
    sasl_mechanism: 'PLAIN',
    username: '',
    password: '',
    schema_registry_url: '',
    schema_registry_username: '',
    schema_registry_password: '',
    ssl_ca_location: '',
    ssl_cert_location: '',
    ssl_key_location: '',
    ssl_key_password: '',
    tls_skip_verify: false,
  };
}
