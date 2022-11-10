import sqlite3
import pandas as pd


class DB:
    def __init__(self):
        self._name = 'mydb.sqlite'

    def execute(self, sql):
        con = sqlite3.connect(self._name)
        con.executescript(sql)

    def read(self, sql):
        con = sqlite3.connect(self._name)
        return pd.read_sql(sql, con)


def drop_all_tables(database):
    database.execute('''
        DROP TABLE IF EXISTS user;
        DROP TABLE IF EXISTS course;
        DROP TABLE IF EXISTS course_attempt;
        DROP TABLE IF EXISTS course_task;
        DROP TABLE IF EXISTS course_task_answer;
        DROP TABLE IF EXISTS attempt_task;
        DROP TABLE IF EXISTS attempt_task_answer;
    ''')


def create_tables(database):
    database.execute('''
        CREATE TABLE IF NOT EXISTS user(
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name VARCHAR(200) NOT NULL  
        );
        
        CREATE TABLE IF NOT EXISTS course(
            course_id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_name VARCHAR(200) NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS course_attempt(
            attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            course_id INTEGER NOT NULL,
            attempt_start DATE NOT NULL,
            attempt_end DATE,
            attempt_mark INTEGER,
            FOREIGN KEY(user_id) REFERENCES user(user_id),
            FOREIGN KEY(course_id) REFERENCES course(course_id)
        );
        
        CREATE TABLE IF NOT EXISTS course_task(
            task_id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_description TEXT,
            course_id INTEGER NOT NULL,
            FOREIGN KEY(course_id) REFERENCES course(course_id)
        );
        
        CREATE TABLE IF NOT EXISTS task_answer(
            answer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            is_correct INTEGER NOT NULL,
            answer_description TEXT,
            FOREIGN KEY(task_id) REFERENCES course_task(task_id)
        );
        
        CREATE TABLE IF NOT EXISTS attempt_task(
            attempt_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL,
            PRIMARY KEY (attempt_id, task_id),
            FOREIGN KEY(attempt_id) REFERENCES course_attempt(attempt_id),
            FOREIGN KEY(task_id) REFERENCES course_task(task_id)
        );
        
        CREATE TABLE IF NOT EXISTS attempt_task_answer(
            answer_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL,
            attempt_id INTEGER NOT NULL,
            PRIMARY KEY (answer_id, attempt_id, task_id),
            FOREIGN KEY(attempt_id) REFERENCES course_attempt(attempt_id),
            FOREIGN KEY(task_id) REFERENCES course_task(task_id),            
            FOREIGN KEY(answer_id) REFERENCES task_answer(answer_id)
        );
    ''')


def select_all_users(database):
    return database.read('''
        SELECT * FROM user;
    ''')


def select_all_courses(database):
    return database.read('''
        SELECT * FROM course;
    ''')


def select_all_attempts(database):
    return database.read('''
        SELECT attempt_id, course_name, user_name, attempt_start, attempt_end, attempt_mark FROM course_attempt
        JOIN course USING (course_id)
        JOIN user USING (user_id);
    ''')


def select_all_tasks(database):
    return database.read('''
        SELECT * FROM course_task;
    ''')


def select_all_attempt_tasks(database):
    return database.read('''
        SELECT course.course_name, user.user_name, course_task.task_description FROM attempt_task
        JOIN course_attempt ON attempt_task.attempt_id = course_attempt.attempt_id
        JOIN user ON course_attempt.user_id = user.user_id
        JOIN course ON course_attempt.course_id = course.course_id
        JOIN course_task ON attempt_task.task_id = course_task.task_id;
    ''')


def select_all_task_answers(database):
    return database.read('''
        SELECT task_description, is_correct, answer_description FROM task_answer
        JOIN course_task USING (task_id);
    ''')


def select_all_attempt_task_answers(database):
    return database.read('''
        SELECT * FROM attempt_task_answer;
    ''')


def populate_users(database):
    names = 'Кононова Софья Богдановна,Кузин Артём Александрович,Сергеев Вадим Маркович,Назарова Ангелина Глебовна,Сорокина Софья Ивановна,Буров Виктор Глебович,Гусев Михаил Львович,Мельников Николай Александрович,Дроздова Мелания Михайловна,Киселев Александр Максимович,Беляева Агата Андреевна,Павлов Константин Романович,Ковалев Михаил Дмитриевич,Селезнева Виктория Адамовна,Петрова Анастасия Дмитриевна,Фомин Антон Михайлович,Серова Василиса Львовна,Суворов Глеб Артёмович,Федорова Олеся Ярославовна,Калинин Андрей Ильич'.split(',')
    for name in names:
        database.execute(f'''
            INSERT INTO user(user_name) VALUES('{name}');
        ''')


def populate_courses(database):
    names = 'Маркетинг,Программирование,Бухгалтерия,Управление,Экономика'.split(',')
    for name in names:
        database.execute(f'''
            INSERT INTO course(course_name) VALUES('{name}');
        ''')


def populate_attempts(database):
    values = [
        [1, 1, '2022-10-19', '2022-10-19', 5],
        [1, 2, '2022-10-20', '2022-10-20', 4],
        [1, 2, '2022-10-21', 'NULL', 'NULL'],
        [2, 2, '2022-10-19', '2022-10-20', 5],
        [2, 2, '2022-10-19', '2022-10-20', 4],
        [3, 2, '2022-10-19', 'NULL', 'NULL'],
        [4, 3, '2022-10-19', 'NULL', 'NULL'],
        [5, 4, '2022-10-22', '2022-10-22', 3],
        [6, 4, '2022-10-23', '2022-10-23', 3],
        [7, 5, '2022-10-24', '2022-10-24', 4],
        [8, 5, '2022-10-25', '2022-10-25', 5],
    ]
    for value in values:
        database.execute(f'''
                    INSERT INTO course_attempt(user_id, course_id, attempt_start, attempt_end, attempt_mark) VALUES({value[0]}, {value[1]}, '{value[2]}', '{value[3]}', {value[4]});
                ''')


def populate_course_tasks(database):
    def create_task_for_course(number, task_number):
        return [number, f'Описание задания под номером {task_number + 1} для курса под номером {number + 1}']

    def create_tasks_for_course(number, task_amount):
        return [create_task_for_course(number, i) for i in range(task_amount)]

    for i in range(1, 6):
        values = create_tasks_for_course(i, 5)
        for value in values:
            database.execute(f'''
                        INSERT INTO course_task(course_id, task_description) VALUES({value[0]}, '{value[1]}');
                    ''')


def populate_attempt_tasks(database):
    values = [
        [1, 1],
        [1, 2],
        [1, 3],
        [2, 2],
        [2, 3],
        [2, 4],
        [3, 3],
        [3, 4],
        [3, 5],
        [4, 1],
        [4, 3],
        [4, 5],
        [5, 2],
        [5, 4],
        [5, 5],
        [6, 1],
        [6, 4],
        [6, 5],
        [7, 1],
        [7, 2],
        [7, 5],
        [8, 2],
        [8, 3],
        [8, 5],
        [9, 1],
        [9, 2],
        [9, 3],
        [10, 1],
        [10, 2],
        [10, 5],
        [11, 2],
        [11, 3],
        [11, 4],
    ]
    for value in values:
        database.execute(f'''
                    INSERT INTO attempt_task(attempt_id, task_id) VALUES({value[0]}, '{value[1]}');
                ''')


def populate_task_answers(database):
    def generate_4_answers_with_correct(task_number, correct_number):
        result = [[task_number, 0, 'Неправильный'] for i in range(4)]
        result[correct_number][1] = 1
        result[correct_number][2] = 'Правильный'
        return result

    for i in range(11):
        for j in range(3):
            for value in generate_4_answers_with_correct(i * 11 + j + 1, j + 1):
                database.execute(f'''
                                INSERT INTO task_answer(task_id, is_correct, answer_description) VALUES({value[0]}, {value[1]}, '{value[2]}');
                            ''')

def populate_task_attempt_answers(database):
    values = [
        [36, 9, 3],
        [38, 10, 3],
        [42, 11, 3],
    ]
    for value in values:
        database.execute(f'''
                        INSERT INTO attempt_task_answer(answer_id, task_id, attempt_id) VALUES({value[0]}, {value[1]}, {value[2]});
                    ''')


def populate_database(database):
    populate_users(database)
    populate_courses(database)
    populate_attempts(database)
    populate_course_tasks(database)
    populate_attempt_tasks(database)
    populate_task_answers(database)
    populate_task_attempt_answers(database)


def select_all_users_that_attended_any_course(database):
    return database.read(f'''
        SELECT DISTINCT user_name FROM user
        JOIN course_attempt USING (user_id);
    ''')


def select_open_attempts_and_sort_by_start_date(database):
    return database.read(f'''
        SELECT course_name, user_name, attempt_start, attempt_end FROM course_attempt
        JOIN course USING (course_id)
        JOIN user USING (user_id)
        WHERE attempt_end = 'NULL'
        ORDER BY attempt_start;
    ''')


def select_courses_and_sort_them_by_attempt_count(database):
    return database.read(f'''
        SELECT course_name, count(course_id) FROM course
        JOIN course_attempt USING (course_id)
        GROUP BY (course_id)
        ORDER BY (count(course_id)) DESC;
    ''')


def select_average_mark_for_course(database):
    return database.read(f'''
        SELECT course_name, avg(attempt_mark) FROM course
        JOIN course_attempt USING (course_id)
        GROUP BY (course_id);
    ''')


def select_attempts_witch_mark_is_less_or_equal_than_average(database):
    return database.read(f'''
        SELECT course_name, user_name, attempt_mark FROM course_attempt
        JOIN course USING (course_id)
        JOIN user USING (user_id)
        WHERE attempt_mark <= (SELECT avg(attempt_mark) FROM course
            JOIN course_attempt USING (course_id)
            WHERE course_attempt.course_id = course.course_id);
    ''')


def select_users_witch_have_opened_attempts(database):
    return database.read(f'''
        SELECT user_name FROM user
        WHERE user_id IN (SELECT DISTINCT user_id FROM course_attempt
                JOIN user USING (user_id)
                WHERE attempt_end = 'NULL'
            );
    ''')


def close_all_attempts_that_are_longer_then_a_week(database):
    database.execute(f'''
        UPDATE course_attempt
        SET attempt_end = DATE('now')
        WHERE attempt_end = 'NULL' AND (julianday('now') - julianday(attempt_start)) > 7;
    ''')
    return select_all_attempts(database)


def set_marks_for_attempt_that_are_closed(database):
    database.execute(f'''
        UPDATE course_attempt
        SET attempt_mark = round(
            5
            *
            (
                SELECT count(attempt_task_answer.answer_id) FROM attempt_task_answer
                JOIN task_answer USING (answer_id)
                WHERE attempt_task_answer.attempt_id = course_attempt.attempt_id AND is_correct > 0
            )
            /
            (
                SELECT count(task_id) FROM attempt_task
                WHERE course_attempt.attempt_id = attempt_task.attempt_id
            )
        )
        WHERE attempt_end != 'NULL' AND attempt_mark IS NULL; 
    ''')
    return select_all_attempts(database)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    database = DB()
    drop_all_tables(database)
    create_tables(database)
    populate_database(database)

    print(select_all_users_that_attended_any_course(database))
    print(select_open_attempts_and_sort_by_start_date(database))

    print(select_courses_and_sort_them_by_attempt_count(database))
    print(select_average_mark_for_course(database))

    print(select_attempts_witch_mark_is_less_or_equal_than_average(database))
    print(select_users_witch_have_opened_attempts(database))

    print(close_all_attempts_that_are_longer_then_a_week(database))
    print(set_marks_for_attempt_that_are_closed(database))

    #print(select_all_users(database))
    #print(select_all_courses(database))
    #print(select_all_attempts(database))
    #print(select_all_tasks(database))
    #print(select_all_attempt_tasks(database))
    #print(select_all_task_answers(database))
    #print(select_all_attempt_task_answers(database))
