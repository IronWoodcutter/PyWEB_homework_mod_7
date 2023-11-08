from datetime import datetime, timedelta
from random import randint, choice
from faker import Faker

from conf import models
from conf.db import session, engine
from conf.models import Student, Group, Subject, Teacher, Grade, Base

NUMBER_TEACHERS = 5
NUMBER_STUDENTS = 50

SUBJECTS = [
    'Астрономія', 'Заклинання', 'Зілляварення',
    'Історія магії', 'Травологія', 'Трансфігурація',
    'Польоти на мітлах', 'Захист від темних мистецтв'
]

GROUPS = ['Гриффіндор', 'Пуффендуй', 'Слізерін']

fake = Faker()

# Створення таблиць в базі даних (якщо вони ще не існують)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


# Створення викладачів
def seed_teachers():
    session.query(models.Teacher).delete()
    session.commit()
    teachers = [Teacher(fullname=fake.name()) for _ in range(NUMBER_TEACHERS)]
    session.add_all(teachers)
    session.commit()


# Створення предметів
def seed_subjects():
    session.query(models.Subject).delete()
    session.commit()
    teachers = session.query(Teacher).all()
    subjects = [Subject(name=subject_name, teacher_id=choice(teachers).id) for subject_name in SUBJECTS]
    session.add_all(subjects)
    session.commit()


# Створення груп
def seed_groups():
    session.query(models.Group).delete()
    session.commit()
    groups = [Group(name=group_name) for group_name in GROUPS]
    session.add_all(groups)
    session.commit()


# Створення студентів
def seed_students():
    session.query(models.Student).delete()
    session.commit()
    groups = session.query(Group).all()
    students = [Student(fullname=fake.name(), group_id=choice(groups).id) for _ in range(NUMBER_STUDENTS)]
    session.add_all(students)
    session.commit()


# Створення оцінок
def seed_grades():
    start_date = datetime.strptime('2022-09-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-06-15', '%Y-%m-%d')

    def get_list_date(start, end):
        result = []
        current_date = start
        while current_date <= end:
            if current_date.isoweekday() < 6:
                result.append(current_date)
            current_date += timedelta(1)

        return result

    list_dates = get_list_date(start_date, end_date)

    for day in list_dates:
        students = session.query(Student).all()
        subjects = session.query(Subject).all()
        # random_subject = choice(subjects).id
        random_students = [choice(students).id for _ in range(randint(3, 7))]

        for student_id in random_students:
            grade = Grade(
                subjects_id=choice(subjects).id,
                student_id=student_id,
                grade=randint(1, 100),
                grade_date=day.date()
            )
            session.add(grade)

    session.commit()


def seed_db():
    seed_teachers()
    seed_subjects()
    seed_groups()
    seed_students()
    seed_grades()


if __name__ == '__main__':
    seed_db()

