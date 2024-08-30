from faker import Faker
import shortuuid
from datetime import datetime
import psycopg2

def create_fake_user() -> dict:
    fake = Faker()
    fake_profile = fake.profile()
    key_list = ["name", "job", "residence", "blood_group", "sex", "birthdate"]
    fake_dict = {key: fake_profile[key] for key in key_list}
    fake_dict.update({
        "phone_number": fake.phone_number(),
        "email": fake.email(),
        "birthdate": fake_dict['birthdate'].strftime("%Y%m%d"),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    return fake_dict

def insert_into_postgres(user_data):
    conn = psycopg2.connect(
        dbname="fake-data", 
        user="airflow", 
        password="airflow", 
        host="localhost", 
        port="5432"
    )
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO users (name, job, residence, blood_group, sex, birthdate, phone_number, email, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for user in user_data:
        cursor.execute(insert_query, (
            user["uuid"], user["name"], user["job"], user["residence"], 
            user["blood_group"], user["sex"], user["birthdate"], 
            user["phone_number"], user["email"], user["timestamp"]
        ))
    conn.commit()
    cursor.close()
    conn.close()

def generate_and_store_data(num_records: int):
    fake_users = [create_fake_user() for _ in range(num_records)]
    insert_into_postgres(fake_users)

if __name__ == "__main__":
    generate_and_store_data(30)