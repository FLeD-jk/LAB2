from model import Model
from view import View
class Controller_SubCategory:
    def __init__(self):
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            choice = self.show_submenu()
            if choice == '1':
                self.add_SubCategory()
            elif choice == '2':
                self.view_SubCategory()
            elif choice == '3':
                self.update_SubCategory()
            elif choice == '4':
                self.delete_SubCategory()
            elif choice == '5':
                break

    def show_submenu(self):
        self.view.show_message("\nSub Menu SubCategory :")
        self.view.show_message("1. Add SubCategory")
        self.view.show_message("2. View SubCategories")
        self.view.show_message("3. Update SubCategory")
        self.view.show_message("4. Delete SubCategory")
        self.view.show_message("5. Quit")
        return input("Enter your choice: ")

    def add_SubCategory(self):
        SubCategory_id, Name ,Category_id  = self.view.get_SubCategory_input()

        if not (Category_id.isdigit() or  SubCategory_id.isdigit()):
            self.view.show_message("Error: 'SubCategory_id'/'Category_id' must be a number.")
            return

        if len(Name) > 30:
            self.view.show_message("Error: The value of the 'Name' column exceeds 30 characters.")
            return

        result_message = self.model.add_SubCategory(SubCategory_id, Name, Category_id)
        self.view.show_message(result_message)

    def view_SubCategory(self):
        SubCategories = self.model.get_all_SubCategory()
        self.view.show_SubCategory(SubCategories)

    def update_SubCategory(self):
        SubCategory_id = self.view.get_id()
        if not SubCategory_id.isdigit():
            self.view.show_message("Error: 'SubCategory_id' must be a number.")
            return
        Name , Category_id = self.view.get_SubCategory_update()
        if len(Name) > 30:
            self.view.show_message("Error: The value of the 'Name' column exceeds 30 characters.")
            return
        if not Category_id.isdigit():
            self.view.show_message("Error: 'Category_id' must be a number.")
            return

        result_message = self.model.update_SubCategory(SubCategory_id, Name, Category_id)
        self.view.show_message(result_message)



    def delete_SubCategory(self):
        SubCategory_id = self.view.get_id()
        if not SubCategory_id.isdigit():
            self.view.show_message("Error: 'SubCategory_id' must be a number.")
            return

        result_message =  self.model.delete_SubCategory(SubCategory_id)
        self.view.show_message(result_message)