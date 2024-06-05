import decimal
import psycopg2
from neo4j import GraphDatabase

def connect_postgres(host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        print("Conexão com PostgreSQL estabelecida com sucesso.")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

# Função para executar uma consulta no PostgreSQL
def query_postgres(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        print("Consulta SQL realizada com sucesso")
        return results
    except Exception as e:
        print(f"Erro ao executar consulta no PostgreSQL: {e}")
        return None

def insert_data_postgres(conn, insert_query):
    try:
        cursor = conn.cursor()
        cursor.execute(insert_query)
        conn.commit()
        cursor.close()
        print("Dados inseridos no Postgres com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir um dado no PostgreSQL: {e}")

def convert_decimal_to_float(data):
    if isinstance(data, dict):
        return {k: convert_decimal_to_float(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_decimal_to_float(i) for i in data]
    elif isinstance(data, decimal.Decimal):
        return float(data)
    else:
        return data

# Função para fechar a conexão com o PostgreSQL
def close_postgres(conn):
    if conn:
        conn.close()
        print("Conexão com PostgreSQL fechada.")

def connect_neo4j(uri, user, password):
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        print("Conexão com Neo4j estabelecida com sucesso.")
        return driver
    except Exception as e:
        print(f"Erro ao conectar ao Neo4j: {e}")
        return None

# Função para inserir dados no Neo4j
def insert_into_neo4j(driver, query, parameters=None):
    try:
        with driver.session() as session:
            session.run(query, parameters)
        print("Dados inseridos no Neo4j com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir dados no Neo4j: {e}")

# Consulta Neo4J
def query_neo4j(driver, query, parameters=None):
    try:
        with driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]
    except Exception as e:
        print(f"Erro ao executar consulta no Neo4j: {e}")
        return None

def clear_database(driver):
    clear_query = """
    MATCH (n)
    DETACH DELETE n
    """
    try:
        with driver.session() as session:
            session.run(clear_query)
        print("Banco Neo4J limpo com sucesso.")
    except Exception as e:
        print(f"Erro ao limpar o banco de dados: {e}")

def close_neo4j(driver):
    if driver:
        driver.close()
        print("Conexão com Neo4j fechada.")

if __name__ == "__main__":

    # Informações de conexão
    postgres_info = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': '1234'
    }

    neo4j_info = {
        'uri': 'bolt://localhost:7687',
        'user': 'neo4j',
        'password': '12345678'
    }

    # Conectando ao PostgreSQL
    postgres_conn = connect_postgres(**postgres_info)

    # Conectando ao Neo4j
    neo4j_driver = connect_neo4j(neo4j_info['uri'], neo4j_info['user'], neo4j_info['password'])

    # Inserindo dados no Postgres
    file = open("smallRelationsInsertFile.sql", "r")
    sqlFile = file.read()
    file.close()

    sqlCommands = sqlFile.split(";")
    for command in sqlCommands:
        insert_data_postgres(postgres_conn, command)
        
    # Recuperando dados do Postgres e inserindo no Neo4J

    if postgres_conn and neo4j_driver:
        # Consulta no Postgres
        advisor_query = "SELECT * FROM advisor"
        results_advisor = query_postgres(postgres_conn, advisor_query)

        classroom_query = "SELECT * FROM classroom"
        results_classroom = query_postgres(postgres_conn, classroom_query)

        course_query = "SELECT * FROM course"
        results_course = query_postgres(postgres_conn, course_query)

        department_query = "SELECT * FROM department"
        results_department = query_postgres(postgres_conn, department_query)
       
        instructor_query = "SELECT * FROM instructor"
        results_instructor = query_postgres(postgres_conn, instructor_query)

        prereq_query = "SELECT * FROM prereq"
        results_prereq = query_postgres(postgres_conn, prereq_query)

        section_query = "SELECT * FROM section"
        results_section = query_postgres(postgres_conn, section_query)

        student_query = "SELECT * FROM student"
        results_student = query_postgres(postgres_conn, student_query)

        takes_query = "SELECT * FROM takes"
        results_takes = query_postgres(postgres_conn, takes_query)

        teaches_query = "SELECT * FROM teaches"
        results_teaches = query_postgres(postgres_conn, teaches_query)

        time_slot_query = "SELECT * FROM time_slot"
        results_time_slot = query_postgres(postgres_conn, time_slot_query)

        # Limpando Banco Neo4J
        clear_database(neo4j_driver)

        # Inserindo resultados no Neo4j
        for row in results_advisor:
            sid, iid = row
            neo4j_query = """
            CREATE (:Advisor {sid: $sid, iid: $iid})
            """
            parameters = convert_decimal_to_float({"sid": sid, "iid": iid})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        for row in results_classroom:
            building, roomNumber, capacity = row
            neo4j_query = """
            CREATE (:Classroom {building: $building, roomNumber: $roomNumber, capacity: $capacity})
            """
            parameters = convert_decimal_to_float({"building": building, "roomNumber": roomNumber, "capacity": capacity})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        for row in results_course:
            courseId, title, deptName, credits = row
            neo4j_query = """
            CREATE (:Course {courseId: $courseId, title: $title, deptName: $deptName, credits: $credits})
            """
            parameters = convert_decimal_to_float({"courseId": courseId, "title": title, "deptName": deptName, "credits": credits})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        for row in results_department:
            deptName, building, budget = row
            neo4j_query = """
            CREATE (:Department {deptName: $deptName, building: $building, budget: $budget})
            """
            parameters = convert_decimal_to_float({"deptName": deptName, "building": building, "budget": budget})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)
        
        for row in results_instructor:
            id, name, deptName, salary = row
            neo4j_query = """
            CREATE (:Instructor {id: $id, name: $name, deptName: $deptName, salary: $salary})
            """
            parameters = convert_decimal_to_float({"id": id, "name": name, "deptName": deptName, "salary": salary})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        for row in results_prereq:
            courseId, prereqId = row
            neo4j_query = """
            CREATE (:Prereq {courseId: $courseId, prereqId: $prereqId})
            """
            parameters = convert_decimal_to_float({"courseId": courseId, "prereqId": prereqId})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        for row in results_section:
            courseId, secId, semester, year, building, roomNumber, timeSlotId = row
            neo4j_query = """
            CREATE (:Section {courseId: $courseId, secId: $secId, semester: $semester, year: $year, building : $building, roomNumber: $roomNumber, timeSlotId: $timeSlotId})
            """
            parameters = convert_decimal_to_float({"courseId": courseId, "secId": secId, "semester": semester, "year": year, "building" : building, "roomNumber": roomNumber, "timeSlotId": timeSlotId})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        for row in results_student:
            id, name, deptName, totCred = row
            neo4j_query = """
            CREATE (:Student {id: $id, name: $name, deptName: $deptName, totCred: $totCred})
            """
            parameters = convert_decimal_to_float({"id": id, "name": name, "deptName": deptName, "totCred": totCred})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        for row in results_takes:
            id, courseId, secId, semester, year, grade = row
            neo4j_query = """
            CREATE (:Takes {id: $id, courseId: $courseId, secId: $secId, semester: $semester, year: $year, grade: $grade})
            """
            parameters = convert_decimal_to_float({"id": id, "courseId": courseId, "secId": secId, "semester": semester, "year": year, "grade": grade})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        for row in results_teaches:
            id, courseId, secId, semester, year = row
            neo4j_query = """
            CREATE (:Teaches {id: $id, courseId: $courseId, secId: $secId, semester: $semester, year: $year})
            """
            parameters = convert_decimal_to_float({"id": id, "courseId": courseId, "secId": secId, "semester": semester, "year": year})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)
        
        for row in results_time_slot:
            timeSlotId, day, startHr, startMin, endHr, endMin = row
            neo4j_query = """
            CREATE (:TimeSlot {timeSlotId: $timeSlotId, day: $day, startHr: $startHr, startMin: $startMin, endHr: $endHr, endMin: $endMin})
            """
            parameters = convert_decimal_to_float({"timeSlotId": timeSlotId, "day": day, "startHr": startHr, "startMin": startMin, "endHr": endHr, "endMin": endMin})
            insert_into_neo4j(neo4j_driver, neo4j_query, parameters)

        # Realizando Consultas no Neo4J

        ## Q1) Listar todos os cursos oferecidos por um determinado departamento

        neo4j_query1 = """
        MATCH (c:Course {deptName: 'Music'})
        RETURN c.courseId AS courseId, c.title AS title, c.deptName AS deptName, c.credits AS credits
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            print("Cursos oferecidos pelo departamento de 'Music':")
            for record in results:
                print(f"Course ID: {record['courseId']}, Title: {record['title']}, Department: {record['deptName']}, Credits: {record['credits']}")
        else:
            print("Nenhum curso encontrado para o departamento 'Music'.")

        ## Q2) Recuperar todas as disciplinas de um curso específico em um determinado semestre

        neo4j_query1 = """
        MATCH (c:Course {title: 'Computational Biology'})-[:OFFERED_IN]->(s:Section {semester: 'Spring'})
        RETURN c.courseId AS courseId, c.title AS courseTitle, s.secId AS sectionId, s.year AS year, s.building AS building, s.roomNumber AS roomNumber, s.timeSlotId AS timeSlotId
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            print("Disciplinas do curso 'Computational Biology' no semestre 'Spring':")
            for record in results:
                print(f"Course ID: {record['courseId']}, Course Title: {record['courseTitle']}, Section ID: {record['sectionId']}, Year: {record['year']}, Building: {record['building']}, Room Number: {record['roomNumber']}, Time Slot ID: {record['timeSlotId']}")
        else:
            print("Nenhuma disciplina encontrada para o curso 'Computational Biology' no semestre 'Spring'.")

        ## Q3) Encontrar todos os estudantes que estão matriculados em um curso específico
        
        neo4j_query1 = """
        MATCH (s:Student)-[:TAKES]->(sec:Section)-[:OFFERED_IN]->(c:Course {title: 'Computational Biology'})
        RETURN s.id AS studentId, s.name AS studentName, c.courseId AS courseId, c.title AS courseTitle
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            print("Estudantes matriculados no curso 'Computational Biology':")
            for record in results:
                print(f"Student ID: {record['studentId']}, Student Name: {record['studentName']}, Course ID: {record['courseId']}, Course Title: {record['courseTitle']}")
        else:
            print("Nenhum estudante encontrado para o curso 'Computational Biology'.")

        ## Q4) Listar a média de salários de todos os professores em um determinado departamento

        neo4j_query1 = """
            MATCH (s:Instructor)
            WHERE s.deptName = 'Biology'
            RETURN s.salary AS salary
            """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Calculando a média de salários
        count = 0
        sum_salaries = 0
        for record in results:
            sum_salaries += record['salary']
            count += 1

        if count > 0:
            average_salary = sum_salaries / count
            print(f"A média de salários de todos os professores do departamento de 'Biology' é de R${average_salary:.2f}")
        else:
            print("Nenhum professor encontrado no departamento de 'Biology'.")       
        
        ## Q5) Recuperar o número total de créditos obtidos por um estudante específico

        neo4j_query1 = """
        MATCH (s:Student {name: 'Shankar'})-[:TAKES]->(sec:Section)-[:OFFERED_IN]->(c:Course)
        RETURN s.id AS studentId, s.name AS studentName, SUM(c.credits) AS totalCredits
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            for record in results:
                print(f"Student ID: {record['studentId']}, Student Name: {record['studentName']}, Total Credits: {record['totalCredits']}")
        else:
            print("Nenhum dado encontrado para o estudante 'Shankar'.")

        ## Q6) Encontrar todas as disciplinas ministradas por um professor em um semestre específico

        neo4j_query1 = """
        MATCH (i:Instructor {name: 'Zhang'})-[:TEACHES]->(sec:Section {semester: 'Spring'})-[:OFFERED_IN]->(c:Course)
        RETURN c.courseId AS courseId, c.title AS courseTitle, sec.secId AS sectionId, sec.semester AS semester, sec.year AS year
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            for record in results:
                print(f"Course ID: {record['courseId']}, Course Title: {record['courseTitle']}, Section ID: {record['sectionId']}, Semester: {record['semester']}, Year: {record['year']}")
        else:
            print("Nenhum dado encontrado para o professor 'Zhang' no semestre 'Spring'.")

        ## Q7) Listar todos os estudantes que têm um determinado professor como orientador
        neo4j_query1 = """
        MATCH (i:Instructor {name: 'Mozart'})<-[:ADVISOR]-(s:Student)
        RETURN s.studentId AS studentId, s.name AS studentName
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            print("Estudantes orientados pelo professor 'Mozart':")
            for record in results:
                print(f"Student ID: {record['studentId']}, Student Name: {record['studentName']}")
        else:
            print("Nenhum estudante encontrado com o professor 'Mozart' como orientador.")
       
        ## Q8) Recuperar todas as salas de aula sem um curso associado
        neo4j_query1 = """
        MATCH (c:Classroom)
        WHERE NOT EXISTS {
            MATCH (c)-[:CLASSROOM_SECTION]->(:Section)
        }
        RETURN c.building AS building, c.roomNumber AS roomNumber, c.capacity AS capacity
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            print("Salas de aula sem um curso associado:")
            for record in results:
                print(f"Building: {record['building']}, Room Number: {record['roomNumber']}, Capacity: {record['capacity']}")
        else:
            print("Nenhuma sala de aula encontrada sem um curso associado.")
       
        ## Q9) Encontrar todos os pré-requisitos de um curso específico

        neo4j_query1 = """
        MATCH (c:Course {title: 'Comp. Sci.'})-[:PREREQ*]->(prerequisite:Course)
        RETURN prerequisite.courseId AS courseId, prerequisite.title AS title
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            print("Pré-requisitos do curso 'Comp. Sci.':")
            for record in results:
                print(f"Course ID: {record['courseId']}, Title: {record['title']}")
        else:
            print("Nenhum pré-requisito encontrado para o curso 'Comp. Sci.'.")

        ## Q10) Recuperar a quantidade de alunos orientados por cada professor

        neo4j_query1 = """
        MATCH (professor:Instructor)-[:ADVISOR]->(student:Student)
        RETURN professor.name AS professor, COUNT(student) AS num_students
        """

        results = query_neo4j(neo4j_driver, neo4j_query1)

        # Imprimindo os resultados
        if results:
            print("Quantidade de alunos orientados por cada professor:")
            for record in results:
                print(f"Professor: {record['professor']}, Número de alunos: {record['num_students']}")
        else:
            print("Nenhum professor com alunos orientados encontrado.")

    # Fechando as conexões
    close_postgres(postgres_conn)
    close_neo4j(neo4j_driver)