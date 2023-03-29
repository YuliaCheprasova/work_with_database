import psycopg2


def create_database():
    """function of a request to create database """

    print("Введите название для новой базы данных")
    name = input()
    create_database_q = f"CREATE DATABASE {name};"
    cursor.execute(create_database_q)
    print(f"Database {name} is created")
    return name


def drop_database(name):
    """function of a request to drop database named 'name' """

    drop_database_q = f"DROP DATABASE {name};"
    cursor.execute(drop_database_q)
    print(f"Database {name} is dropped")

def show_databases():
    """function of a request to show exist databases """

    show_databases_q = "SELECT datname FROM pg_database;"
    cursor.execute(show_databases_q)
    print(f"Exist databases: {cursor.fetchall()}")

def show_table(name):
    """function of a request to show data from a table named as 'name' """

    print(f"\nTable {name}")
    show_table_q = f"SELECT * FROM {name};"
    cursor.execute(show_table_q)
    for tuple in cursor.fetchall():
        print(tuple)
    print("\n")

def show_columns(name):
    """function of a request to show columns of table name as 'name' """

    show_columns_q = f"select column_name from information_schema.columns where table_name = '{name}' ;"
    cursor.execute(show_columns_q)
    print(f"Columns of table {name}:", cursor.fetchall())

def drop_table(name):
    drop_table_q = f"DROP TABLE {name};"
    cursor.execute(drop_table_q)
    print(f"Table {name} is dropped")


# connection to an exist database
try:
    connection = psycopg2.connect(
        user="postgres",
        password="22424",
        host="127.0.0.1",
        port="5432",
        database="db_for_work_with_python"
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        # example of a request to create a database
        name_of_database = create_database()
        show_databases()
        # example of a request to create a table
        create_table_exhibitions_q="CREATE TABLE exhibitions (exhibition_id SERIAL PRIMARY KEY," \
                                   "name VARCHAR(50) NOT NULL, " \
                                   "duration TIME," \
                                   " age_limit SMALLINT CHECK (age_limit>=0));"
        cursor.execute(create_table_exhibitions_q)
        print("Table exhibition is created")
        # example of a request to fill in a table
        insert_exhibitions_q="INSERT INTO exhibitions(name, duration, age_limit) " \
                             "VALUES ('The Pushkin', '1:50', 16), ('Our_predecessors', '2:30', 12), " \
                             "('The wonders of science', '1:10', 16), ('The World of animals', '1:30', 0);"
        cursor.execute(insert_exhibitions_q)
        print("Values are added in table exhibitions")
        #show result of filling in
        show_table('exhibitions')
        # example of a request to update a table
        update_age_limit_exhibitions_q="UPDATE exhibitions " \
                                       "SET age_limit = 14 " \
                                       "WHERE age_limit = 16;"
        cursor.execute(update_age_limit_exhibitions_q)
        print("Table exhibitions is updated")
        # show result of updating
        show_table('exhibitions')
        # example of a request to delete some data
        delete_pushkin_q="DELETE FROM exhibitions WHERE name = 'The Pushkin';"
        cursor.execute(delete_pushkin_q)
        print("Values are deleted from table exhibitions")
        # show result of deleting
        show_table('exhibitions')
        # create a new table
        create_table_sessions_q = "CREATE TABLE sessions" \
                                  "(session_id SERIAL PRIMARY KEY," \
                                  "date_of_session DATE NOT NULL," \
                                  "exhibition_id INT," \
                                  "FOREIGN KEY (exhibition_id) REFERENCES exhibitions (exhibition_id)" \
                                  "ON DELETE SET NULL);"
        cursor.execute(create_table_sessions_q)
        print("Table sessions is created")
        show_columns('sessions')
        alter_sessions_q = "ALTER TABLE sessions ADD COLUMN time_of_session TIME NOT NULL;"
        cursor.execute(alter_sessions_q)
        print("Table sessions is altered")
        # show result of altering
        show_columns('sessions')
        # fill in sessions
        insert_sessions_q="INSERT INTO sessions(date_of_session, time_of_session, exhibition_id) " \
                             "VALUES ('2023-03-23', '18:00', 4), ('2023-04-01', '21:30', 4), " \
                             "('2023-04-08', '19:00', 3), ('2023-04-11', '18:00', 2), " \
                          "('2023-06-06', '13:00', 4), ('2023-03-29', '13:30', 4);"
        cursor.execute(insert_sessions_q)
        print("Values are added in table sessions")
        # show result of filling in
        show_table('sessions')
        # example of a request to join tables
        join_q="SELECT ex.name, s.date_of_session, s.time_of_session " \
               "FROM exhibitions AS ex " \
               "JOIN sessions AS s " \
               "ON ex.exhibition_id = s.exhibition_id " \
               "WHERE ex.name = 'The World of animals'; "
        cursor.execute(join_q)
        print("The result of joining two tables")
        for tuple in cursor.fetchall():
            print(tuple)
        # example of a request to drop a table
        drop_table('sessions')
        drop_table('exhibitions')
        # example of a request to drop a database
        drop_database(name_of_database)
        show_databases()


except Exception as _ex:
    print("Error while working with PostgreSQL: ", _ex)
finally:
    if connection:
        connection.close()
        print("PostgreSQL connection closed")
