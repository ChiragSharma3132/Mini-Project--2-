const BACKEND_URL = "https://mini-project-2-uyp8.onrender.com".replace(/\/+$/, "");

window.addEventListener('load', () => {
    checkBackend();
    setInterval(checkBackend, 5000);
    loadTables();

    // Ctrl+Enter to run query
    document.getElementById('queryInput').addEventListener('keydown', (e) => {
        if (e.ctrlKey && e.key === 'Enter') executeQuery();
    });

    // Upload tab listener setup
    document.getElementById('csvFileInput').addEventListener('change', handleFileUpload);

    // Drag and drop for upload zone
    const uploadZone = document.getElementById('uploadZone');
    if (uploadZone) {
        uploadZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadZone.classList.add('drag-over');
        });
        uploadZone.addEventListener('dragleave', () => {
            uploadZone.classList.remove('drag-over');
        });
        uploadZone.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadZone.classList.remove('drag-over');
            const file = e.dataTransfer.files[0];
            if (file) {
                const input = document.getElementById('csvFileInput');
                const dt = new DataTransfer();
                dt.items.add(file);
                input.files = dt.files;
                handleFileUpload({ target: input });
            }
        });
    }

    updateLineNumbers();
});

// ── Line numbers ──────────────────────────────────────
function updateLineNumbers() {
    const textarea = document.getElementById('queryInput');
    const lineNumbers = document.getElementById('lineNumbers');
    const lines = textarea.value.split('\n').length;
    lineNumbers.textContent = Array.from({length: lines}, (_, i) => i + 1).join('\n');
}

function syncScroll(el) {
    const lineNumbers = document.getElementById('lineNumbers');
    lineNumbers.scrollTop = el.scrollTop;
}

// ── Backend check ─────────────────────────────────────
async function checkBackend() {
    const statusEl = document.getElementById('backendStatus');
    const dot = statusEl.querySelector('.status-dot');
    const text = statusEl.querySelector('.status-text');
    try {
        const resp = await fetch(`${BACKEND_URL}/health`);
        if (resp.ok) {
            text.textContent = 'Backend Online';
            statusEl.classList.remove('offline');
            statusEl.classList.add('online');
            return true;
        }
    } catch (e) {}
    text.textContent = 'Backend Offline';
    statusEl.classList.remove('online');
    statusEl.classList.add('offline');
    return false;
}

// ── Execute query ─────────────────────────────────────
async function executeQuery() {
    const query = document.getElementById("queryInput").value.trim();
    if (!query) return;

    const resultSection = document.getElementById('resultSection');
    const resultEl = document.getElementById("result");

    resultEl.textContent = "Running…";
    resultSection.style.display = 'block';

    try {
        const response = await fetch(`${BACKEND_URL}/query`, {
            method: "POST",
            headers: { "Content-Type": "text/plain" },
            body: query
        });

        const result = await response.text();
        resultEl.textContent = result;
        addToHistory(query);

        const queryUpper = query.toUpperCase();
        if (queryUpper.startsWith("DROP TABLE") || queryUpper.startsWith("CREATE TABLE")) {
            loadTables();
        }
    } catch (error) {
        resultEl.textContent = "⚠  Could not connect to the backend. Is it running?";
    }
}

function closeResult() {
    document.getElementById('resultSection').style.display = 'none';
}

function clearQuery() {
    document.getElementById("queryInput").value = "";
    updateLineNumbers();
    document.getElementById('resultSection').style.display = 'none';
}

// ── History ───────────────────────────────────────────
function addToHistory(query) {
    const history = document.getElementById("historyList");
    const li = document.createElement("li");
    li.textContent = query;
    li.title = query;
    li.onclick = () => {
        document.getElementById('queryInput').value = query;
        updateLineNumbers();
        switchTab('editor', { target: document.querySelector('.nav-btn') });
    };
    history.prepend(li);
}

function clearHistory() {
    document.getElementById("historyList").innerHTML = '';
}

// ── Tabs ──────────────────────────────────────────────
function switchTab(tabName, event) {
    document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
    document.getElementById(tabName).classList.add('active');
    document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
    if (event && event.target) {
        let btn = event.target.closest('.nav-btn');
        if (btn) btn.classList.add('active');
    }
    const container = document.querySelector('.tab-container');
    if (container) container.scrollTop = 0;

    if (tabName === 'tables') loadTables();
}

// ── Tables ────────────────────────────────────────────
async function loadTables() {
    try {
        const response = await fetch(`${BACKEND_URL}/tables`);
        const tablesText = await response.text();
        const tables = tablesText.trim().split('\n').filter(t => t);
        const container = document.getElementById('tablesContainer');
        container.innerHTML = '';
        clearTablePreview();
        if (tables.length === 0) {
            container.innerHTML = '<p style="color:var(--text-secondary);font-size:13px;">No tables yet. Create one using a CREATE TABLE query.</p>';
        } else {
            tables.forEach(table => {
                const button = document.createElement('button');
                button.textContent = table;
                button.className = 'table-btn';
                button.onclick = () => showTableDetails(table);
                container.appendChild(button);
            });
        }
    } catch (error) {
        document.getElementById('tablesContainer').innerHTML =
            '<p style="color:var(--text-secondary);font-size:13px;">Could not load tables — backend may be offline.</p>';
        clearTablePreview();
    }
}

async function showTableDetails(tableName) {
    if (!tableName) {
        return;
    }

    const preview = document.getElementById('tablePreview');
    const title = document.getElementById('tablePreviewTitle');
    const hint = document.getElementById('tablePreviewHint');
    const descriptionEl = document.getElementById('tableDescription');
    const dataEl = document.getElementById('tableData');

    preview.style.display = 'block';
    title.textContent = `${tableName} Preview`;
    hint.textContent = 'Loading description and rows...';
    descriptionEl.textContent = 'Loading description...';
    dataEl.textContent = 'Loading data...';

    document.querySelectorAll('#tablesContainer .table-btn').forEach(btn => {
        btn.classList.toggle('active-table', btn.textContent === tableName);
    });

    try {
        const encodedTable = encodeURIComponent(tableName);
        const [descResp, dataResp] = await Promise.all([
            fetch(`${BACKEND_URL}/table-description?table=${encodedTable}`),
            fetch(`${BACKEND_URL}/query`, {
                method: 'POST',
                headers: { 'Content-Type': 'text/plain' },
                body: `SELECT * FROM ${tableName};`
            })
        ]);

        const description = await descResp.text();
        const dataText = await dataResp.text();

        descriptionEl.textContent = description || 'No description available.';
        dataEl.textContent = dataText || '(no data)';
        hint.textContent = 'Description and data loaded';
    } catch (error) {
        descriptionEl.textContent = 'Could not load table description. Backend may be offline.';
        dataEl.textContent = 'Could not load table data. Backend may be offline.';
        hint.textContent = 'Load failed';
    }
}

function clearTablePreview() {
    const preview = document.getElementById('tablePreview');
    const title = document.getElementById('tablePreviewTitle');
    const hint = document.getElementById('tablePreviewHint');
    const descriptionEl = document.getElementById('tableDescription');
    const dataEl = document.getElementById('tableData');

    if (!preview || !title || !hint || !descriptionEl || !dataEl) {
        return;
    }

    preview.style.display = 'none';
    title.textContent = 'Table Preview';
    hint.textContent = 'Select a table to view details';
    descriptionEl.textContent = '';
    dataEl.textContent = '';

    document.querySelectorAll('#tablesContainer .table-btn').forEach(btn => {
        btn.classList.remove('active-table');
    });
}

// ── CSV Upload ────────────────────────────────────────
async function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    if (!file.name.endsWith('.csv')) {
        setUploadStatus('Please select a CSV file.', 'error');
        return;
    }

    const reader = new FileReader();
    reader.onload = async (e) => {
        const content = e.target.result;
        const tableName = file.name.replace('.csv', '');
        setUploadStatus('Uploading…', 'info');

        try {
            const response = await fetch(`${BACKEND_URL}/upload`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tableName, content })
            });

            if (response.ok) {
                setUploadStatus(`✓  Table "${tableName}" uploaded successfully!`, 'success');
                loadTables();
            } else {
                setUploadStatus('Upload failed. Check backend logs.', 'error');
            }
        } catch (error) {
            setUploadStatus('Upload error — backend may be offline.', 'error');
        }
    };
    reader.readAsText(file);
}

function setUploadStatus(msg, type) {
    const el = document.getElementById('uploadStatus');
    el.textContent = msg;
    el.style.color = type === 'success' ? 'var(--green)' :
                     type === 'error'   ? 'var(--red)'   :
                                          'var(--text-secondary)';
}

// ── Example Queries ───────────────────────────────────
function insertExample(type) {
    const examples = {
        'create': 'CREATE TABLE students (id INT, name VARCHAR, age FLOAT);',
        'insert': "INSERT INTO students VALUES (1, 'John', 20), (2, 'Jane', 21);",
        'select': 'SELECT * FROM students;',
        'update': 'UPDATE students SET age = 21 WHERE id = 1;',
        'delete': 'DELETE FROM students WHERE id = 1;'
    };
    const ta = document.getElementById('queryInput');
    ta.value = examples[type];
    updateLineNumbers();
    ta.focus();
}

// ── Chatbot ───────────────────────────────────────────
function getLocalBotResponse(input) {
    input = input.toLowerCase();
    if (input.includes("hello") || input.includes("hi")) return "Hello! I can help you understand the DB engine.";
    if (input.includes("select"))   return "SELECT retrieves data from a table. Example: SELECT * FROM students;";
    if (input.includes("insert"))   return "INSERT adds rows to a table. Example: INSERT INTO students VALUES (1, 'Alice', 22);";
    if (input.includes("create"))   return "CREATE TABLE defines a new table with its columns and types.";
    if (input.includes("update"))   return "UPDATE modifies existing rows. Use a WHERE clause to target specific rows.";
    if (input.includes("delete"))   return "DELETE removes rows from a table. Always use WHERE to avoid deleting everything!";
    if (input.includes("index"))    return "Indexing improves search speed using B+ Trees. Use CREATE INDEX to add one.";
    if (input.includes("parser"))   return "The SQL Parser converts a query string into a structured representation for execution.";
    if (input.includes("query"))    return "A query is parsed, planned, and then executed against the stored data.";
    if (input.includes("join"))     return "JOINs combine rows from two or more tables based on a related column.";
    if (input.includes("where"))    return "WHERE filters rows based on a condition. Example: WHERE age > 18";
    if (input.includes("dbms"))     return "A DBMS manages databases, allowing you to create, read, update, and delete data efficiently.";
    return "I answer questions about SQL and this database engine. Try asking about SELECT, INSERT, INDEX, or JOIN!";
}

async function sendMessage() {
    const inputField = document.getElementById("chat-input");
    const message = inputField.value.trim();
    if (!message) return;

    const chatBox = document.getElementById("chat-box");

    chatBox.innerHTML += `
      <div class="chat-message user-message">
        <div class="chat-avatar">YOU</div>
        <div class="chat-bubble">${escapeHtml(message)}</div>
      </div>`;

        let response = '';

        try {
                const apiResponse = await fetch(`${BACKEND_URL}/chat`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'text/plain' },
                        body: message
                });

                response = await apiResponse.text();
                if (!apiResponse.ok || !response.trim()) {
                        throw new Error('Chat endpoint returned an empty response');
                }
        } catch (error) {
                response = getLocalBotResponse(message);
        }

        setTimeout(() => {
        chatBox.innerHTML += `
          <div class="chat-message bot-message">
            <div class="chat-avatar">AI</div>
            <div class="chat-bubble">${escapeHtml(response)}</div>
          </div>`;
        chatBox.scrollTop = chatBox.scrollHeight;
    }, 300);

    inputField.value = "";
    chatBox.scrollTop = chatBox.scrollHeight;
}

function escapeHtml(str) {
    return str
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

