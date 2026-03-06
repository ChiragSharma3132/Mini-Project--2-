let historyList = [];

function switchTab(tabId, event) {
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.remove('active');
    });

    document.getElementById(tabId).classList.add('active');

    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.classList.remove('active');
    });

    event.target.classList.add('active');
}

function executeQuery() {
    let query = document.getElementById("queryInput").value;

    if(query.trim() === "") return;

    historyList.push(query);
    updateHistory();

    alert("Query Executed (Mock)");
}

function clearQuery() {
    document.getElementById("queryInput").value = "";
}

function updateHistory() {
    let historyUI = document.getElementById("history");
    historyUI.innerHTML = "";

    historyList.forEach(q => {
        let li = document.createElement("li");
        li.innerText = q;
        historyUI.appendChild(li);
    });
}

function insertExample(type) {
    let examples = {
        create: "CREATE TABLE students (id INT, name TEXT);",
        insert: "INSERT INTO students VALUES (1, 'Vikram');",
        select: "SELECT * FROM students;",
        update: "UPDATE students SET name='Raj' WHERE id=1;",
        delete: "DELETE FROM students WHERE id=1;"
    };

    document.getElementById("queryInput").value = examples[type];
}