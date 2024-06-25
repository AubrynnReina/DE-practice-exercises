import psycopg2
import glob

def main():

    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here

    create_table_script = '''
    CREATE TABLE IF NOT EXISTS accounts(
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(20),
        last_name VARCHAR(20),
        address_1 VARCHAR(100),
        address_2 VARCHAR(100),
        city VARCHAR(30),
        state VARCHAR(30),
        zip_code VARCHAR(9),
        join_date DATE
    );

    CREATE TABLE IF NOT EXISTS products(
        product_id SERIAL PRIMARY KEY,
        product_code VARCHAR(2),
        product_description VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS transactions(
        transaction_id SERIAL PRIMARY KEY,
        transaction_date DATE,
        product_id SERIAL REFERENCES products(product_id),
        product_code VARCHAR(2),
        product_description VARCHAR(100),
        quantity INT,
        account_id SERIAL REFERENCES accounts(customer_id)
    );

    CREATE INDEX IF NOT EXISTS account_city_idx ON accounts(city);
    CREATE INDEX IF NOT EXISTS product_code_idx ON products(product_code);
    CREATE INDEX IF NOT EXISTS transaction_id_idx ON transactions(transaction_id);
    '''

    cursor = conn.cursor()
    cursor.execute(create_table_script)

    # Ingest data
    files = glob.glob('./data/*.csv')
    for file in files:
        with open(file, 'r') as f:
            table_name = file.split('/')[-1].split('.')[0]
            next(f) # Skip the header row.
            cursor.copy_from(f, table_name, sep=',')

    conn.commit()
    cursor.execute('select * from accounts')

    results = cursor.fetchall()
    print(results)
    
if __name__ == "__main__":
    main()
