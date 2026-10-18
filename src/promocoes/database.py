import sqlite3

def config_database():
    conn = sqlite3.connect('promocoes.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS promotions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            category TEXT NOT NULL
            UNIQUE(email,categoria)
        )
    ''')
    conn.commit()
    conn.close()
