from model import Model
from view import View

class Controller_Category:
    def __init__(self):
        self.model = Model()
        self.view = View()


    def run(self):
        while True:
            choice = self.show_submenu()
            if choice == '1':
                self.add_Category()
            elif choice == '2':
                self.view_Category()
            elif choice == '3':
                self.update_Category()
            elif choice == '4':
                self.delete_Category()
            elif choice == '5':
                break

    def show_submenu(self):
        self.view.show_message("\nSub Menu Category :")
        self.view.show_message("1. Add Category")
        self.view.show_message("2. View Categories")
        self.view.show_message("3. Update Category")
        self.view.show_message("4. Delete Category")
        self.view.show_message("5. Quit")
        return input("Enter your choice: ")

    def add_Category(self):
        Category_id, Name = self.view.get_Category_input()

        if not Category_id.isdigit():
            self.view.show_message("Error: 'Category_id' must be a number.")
            return

        if len(Name) > 30:
            self.view.show_message("Error: The value of the 'Name' column exceeds 30 characters.")
            return

        result_message = self.model.add_Category(Category_id, Name)
        self.view.show_message(result_message)

    def view_Category(self):
        Categories = self.model.get_all_Category()
        self.view.show_Category(Categories)

    def update_Category(self):
        Category_id = self.view.get_id()
        if not Category_id.isdigit():
            self.view.show_message("Error: 'Category_id' must be a number.")
            return
        Name = self.view.get_Category_update()
        if len(Name) > 30:
            self.view.show_message("Error: The value of the 'Name' column exceeds 30 characters.")
            return
        result_message =  self.model.update_Category(Category_id, Name)
        self.view.show_message(result_message)

    def delete_Category(self):
        Category_id = self.view.get_id()
        if not Category_id.isdigit():
            self.view.show_message("Error: 'Category_id' must be a number.")
            return

        result_message = self.model.delete_Category(Category_id)
        self.view.show_message(result_message)
