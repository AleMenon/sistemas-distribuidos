import sqlite3

def config_database():
    conn = sqlite3.connect("estoque.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            quantity INTEGER NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS reservations (
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            PRIMARY KEY (order_id, product_id)
        )
        """
    )

    cursor.execute('SELECT COUNT(*) FROM products')
    if cursor.fetchone()[0] == 0:
        init_products = [
            (1, 'produto_a', 5),
            (2, 'produto_b', 5),
            (3, 'produto_c', 5)
        ]
        cursor.executemany('INSERT INTO products (id, name, quantity) VALUES (?, ?, ?)', init_products)
    
    conn.commit()
    conn.close()
