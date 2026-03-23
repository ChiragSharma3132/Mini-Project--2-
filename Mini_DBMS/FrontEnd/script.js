const BACKEND_URL = "http://localhost:8080";

window.addEventListener('load', () => {
    checkBackend();
    // Keep the UI updated if the backend comes online later
    setInterval(checkBackend, 5000);
    loadTables();
});

async function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    if (!file.name.endsWith('.csv')) {
        alert('Please select a CSV file.');
        return;
    }

    const reader = new FileReader();
    reader.onload = async (e) => {
        const content = e.target.result;
        const tableName = file.name.replace('.csv', '');

        try {
            const statusEl = document.getElementById('uploadStatus');
            statusEl.textContent = 'Uploading...';
            statusEl.style.color = 'blue';

            const response = await fetch(`${BACKEND_URL}/upload`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ tableName, content })
            });

            if (response.ok) {
                statusEl.textContent = `Table '${tableName}' uploaded successfully!`;
                statusEl.style.color = 'green';
                loadTables(); // Refresh the tables list
            } else {
                statusEl.textContent = 'Upload failed.';
                statusEl.style.color = 'red';
            }
        } catch (error) {
            console.error(error);
            document.getElementById('uploadStatus').textContent = 'Upload error.';
            document.getElementById('uploadStatus').style.color = 'red';
        }
    };
    reader.readAsText(file);
}

async function checkBackend() {
    const statusEl = document.getElementById('backendStatus');
    try {
        const resp = await fetch(`${BACKEND_URL}/health`);
        if (resp.ok) {
            statusEl.textContent = 'Backend: Online';
            statusEl.classList.remove('offline');
            statusEl.classList.add('online');
            return true;
        }
    } catch (e) {
        // ignore
    }

    statusEl.textContent = 'Backend: Offline';
    statusEl.classList.remove('online');
    statusEl.classList.add('offline');
    return false;
}

async function executeQuery() {
    const query = document.getElementById("queryInput").value;

    if (!query) {
        alert("Please enter a query");
        return;
    }

    try {

        const response = await fetch(`${BACKEND_URL}/query`, {
            method: "POST",
            headers: {
                "Content-Type": "text/plain"
            },
            body: query
        });

        const result = await response.text();

        document.getElementById("result").innerText = result;

        addToHistory(query);

        // If the query affects table list, refresh it so UI stays in sync
        const queryUpper = query.trim().toUpperCase();
        if (queryUpper.startsWith("DROP TABLE") || queryUpper.startsWith("CREATE TABLE")) {
            loadTables();
        }

    } catch (error) {

        console.error(error);
        alert("Backend connection error");

    }
}

function clearQuery() {
    document.getElementById("queryInput").value = "";
}

function addToHistory(query){

    const history = document.getElementById("historyList");

    const li = document.createElement("li");
    li.innerText = query;

    history.prepend(li);
}

function switchTab(tabName, event) {
    // Hide all tabs
    document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
    // Show the selected tab
    document.getElementById(tabName).classList.add('active');
    // Update nav buttons
    document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
    event.target.classList.add('active');

    // Scroll main content to top so the tab title stays visible
    const main = document.querySelector('.main');
    if (main) main.scrollTop = 0;

    if (tabName === 'tables') {
        loadTables();
    } else if (tabName === 'upload') {
        document.getElementById('csvFileInput').addEventListener('change', handleFileUpload);
    } else {
        // Remove listener when not on upload tab
        document.getElementById('csvFileInput').removeEventListener('change', handleFileUpload);
    }
}

async function loadTables() {
    try {
        const response = await fetch(`${BACKEND_URL}/tables`);
        const tablesText = await response.text();
        const tables = tablesText.trim().split('\n').filter(t => t);
        const container = document.getElementById('tablesContainer');
        container.innerHTML = '';
        if (tables.length === 0) {
            container.innerHTML = '<p>No tables found.</p>';
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
        console.error(error);
        document.getElementById('tablesContainer').innerHTML = '<p>Error loading tables.</p>';
    }
}

function insertExample(type) {
    const examples = {
        'create': 'CREATE TABLE students (id INT, name VARCHAR, age FLOAT);',
        'insert': "INSERT INTO students VALUES (1, 'John', 20), (2, 'Jane', 21);",
        'select': 'SELECT * FROM students;',
        'update': 'UPDATE students SET age = 21 WHERE id = 1;',
        'delete': 'DELETE FROM students WHERE id = 1;'
    };
    document.getElementById('queryInput').value = examples[type];
}