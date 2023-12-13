from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Database:

    def __init__(self):

        engine = create_engine('postgresql://postgres:1111@localhost/LAB2')
        session_class = sessionmaker(bind=engine)
        self.session = session_class()

    def save_all(self, objects):

        self.session.add_all(objects)
        self.session.commit()

    def delete_all(self):

        self.session.query(TodoItem).delete()
        self.session.query(TodoList).delete()