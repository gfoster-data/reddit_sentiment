from sqlalchemy import create_engine, inspect

def list_tables(db_path):
    # Create an engine connected to the SQLite database
    engine = create_engine(f'sqlite:///{db_path}')

    # Use the inspect module to get table information
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    # Print the table names
    for table in tables:
        print(table)

# Example usage
list_tables('reddit_smp_sentiment.db')