from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session
from pprint import pprint


def select_1():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_2():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_3():
    '''
    --3 Знайти середній бал у групах з певного предмета
    SELECT d.name, gr.name , ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN [groups] gr ON gr.id = s.group_id
    WHERE d.id = 8
    GROUP BY gr.name
    ORDER BY average_grade DESC;
    '''
    result = (
        session.query(Subject.name, Group.name, func.ROUND(func.AVG(Grade.grade), 2).label("Average grade"))
        .select_from(Grade)
        .join(Student)
        .join(Subject)
        .join(Group)
        .filter(Subject.id == 8)
        .group_by(Group.name, Subject.name)
        .order_by(desc("Average grade")).all()

    )
    return result


def select_4():
    '''
    --4 Знайти середній бал на потоці (по всій таблиці оцінок)
    SELECT ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g;
    '''
    result = session.query(func.ROUND(func.AVG(Grade.grade), 2).label("Average grade")).select_from(Grade).all()
    return result


def select_5():
    '''
    --5 Знайти які курси читає певний викладач
    SELECT d.name,t.fullname
    FROM disciplines d
    JOIN teachers t ON t.id = d.teacher_id
    WHERE t.id = 4
    GROUP BY d.name;
    '''
    result = session.query(Subject.name, Teacher.fullname)\
              .select_from(Subject)\
              .join(Teacher)\
              .filter(Subject.id == 4)\
              .group_by(Subject.name, Teacher.fullname).all()

    return result


def select_6():
    '''
    --6 Знайти список студентів у певній групі
    SELECT s.fullname, g.name
    FROM students s
    JOIN [groups] g ON g.id = s.group_id
    WHERE g.id = 3
    GROUP BY s.fullname;
    '''
    result = session.query(Student.fullname, Group.name)\
              .select_from(Student)\
              .join(Group)\
              .filter(Group.id == 3)\
              .group_by(Student.fullname, Group.name).all()
    return result


def select_7():
    '''
    --7 Знайти оцінки студентів у окремій групі з певного предмета
    SELECT g.grade, s.fullname, gr.name, d.name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN [groups] gr ON gr.id = s.group_id
    WHERE d.id = 4 AND gr.id = 3
    ORDER BY s.fullname;
    '''
    result = session.query(Grade.grade, Student.fullname, Group.name, Subject.name) \
        .select_from(Student) \
        .join(Grade) \
        .join(Subject) \
        .join(Group) \
        .filter(and_(Subject.id == 4, Group.id == 3)) \
        .group_by(Grade.grade, Student.fullname, Group.name, Subject.name).all()
    return result


def select_8():
    '''
    --8 Знайти середній бал, який ставить певний викладач зі своїх предметів.
    SELECT ROUND(AVG(g.grade), 2) AS average_grade, t.fullname, d.name
    FROM grades g
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN teachers t ON t.id = d.teacher_id
    WHERE t.id = 4
    GROUP BY d.name;
    '''
    result = session.query(Teacher.fullname, Subject.name, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Subject).join(Teacher).filter(Teacher.id == 2) \
        .group_by(Teacher.fullname, Subject.name).order_by(desc('average_grade')).all()

    return result


def select_9():
    '''
    --9 Знайти список курсів, які відвідує студент
    SELECT d.name, s.fullname
    FROM grades g
    JOIN students s ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    WHERE s.id = 41
    GROUP BY d.name;
    '''
    result = session.query(Subject.name, Student.fullname) \
        .select_from(Grade).join(Student).join(Subject).filter(Student.id == 2) \
        .group_by(Subject.name, Student.fullname).all()

    return result


def select_10():
    '''
    --10 Список курсів, які певному студенту читає певний викладач
    SELECT  d.name, s.fullname, t.fullname
    FROM grades g
    JOIN students s ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN teachers t ON t.id = d.teacher_id
    WHERE s.id = 28 AND t.id = 4
    GROUP BY d.name;
    '''
    result = session.query(Subject.name, Student.fullname, Teacher.fullname) \
        .select_from(Grade).join(Student).join(Subject).join(Teacher).filter(and_(Student.id == 9, Teacher.id == 5)) \
        .group_by(Subject.name, Student.fullname, Teacher.fullname).all()

    return result


def select_11():
    '''
    --11 Середній бал, який певний викладач ставить певному студентові
    SELECT t.fullname, s.fullname, ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN teachers t ON t.id = d.teacher_id
    WHERE t.id = 4 AND s.id = 25;
    '''
    result = session.query(Teacher.fullname, Student.fullname,
                           func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).join(Subject).join(Teacher).filter(
        and_(Student.id == 9, Teacher.id == 5)).group_by(Teacher.fullname, Student.fullname).all()
    return result


def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result

