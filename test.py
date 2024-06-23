import psycopg2

from config import host, user, password, db_name


try:
    connection = psycopg2.connect(
        host = host,
        user = user,
        password = password,
        database = db_name
    )
    connection.autocommit = True

    with connection.cursor() as cursor:
        # ID, который вы хотите проверить
        id_to_check = 123

        # Выполнение SQL-запроса для проверки наличия ID в таблице
        cursor.execute("SELECT EXISTS(SELECT 1 FROM authors WHERE authorid = %s)", (id_to_check,))
        exists = cursor.fetchone()[0]

        if exists:
            print(f"ID {id_to_check} найден в таблице.")
        else:
            print(f"ID {id_to_check} не найден в таблице.")

    
        
except Exception as _ex:
    print("Error connecting to PostgreSQL: ", _ex)
finally:
    if connection:
        connection.close()
        print("PostgreSQL connection closed.")

