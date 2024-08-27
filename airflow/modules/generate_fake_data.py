from faker import Faker
import shortuuid
from datetime import datetime

def create_fake_user() -> dict:
    fake = Faker()
    fake_profile = fake.profile()
    
    key_list = ["name", "job", "residence", "blood_group", "sex", "birthdate"]
    fake_dict = {}

    # 선택된 키의 값을 fake_dict에 추가
    for key in key_list:
        fake_dict[key] = fake_profile[key]
        
    fake_dict["phone_number"] = fake.phone_number()
    fake_dict["email"] = fake.email()
    fake_dict["uuid"] = shortuuid.uuid()
    # YYYYMMDD 변환
    fake_dict['birthdate'] = fake_dict['birthdate'].strftime("%Y%m%d")
    fake_dict['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return fake_dict

def generate_fake_data(num_records: int):
    fake_users = []
    for _ in range(num_records):
        user = create_fake_user()
        fake_users.append(user)
    return fake_users

if __name__ == "__main__":
    data = generate_fake_data(30)
    for user in data:
        print(user)