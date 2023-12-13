from model import Model
from view import View
class Controller_Brand:
    def __init__(self):
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            choice = self.show_submenu()
            if choice == '1':
                self.add_Brand()
            elif choice == '2':
                self.view_Brand()
            elif choice == '3':
                self.update_Brand()
            elif choice == '4':
                self.delete_Brand()
            elif choice == '5':
                break

    def show_submenu(self):
        self.view.show_message("\nSub Menu Brand :")
        self.view.show_message("1. Add Brand")
        self.view.show_message("2. View Brand")
        self.view.show_message("3. Update Brand")
        self.view.show_message("4. Delete Brand")
        self.view.show_message("5. Quit")
        return input("Enter your choice: ")

    def add_Brand(self):
        Brand_id, Name = self.view.get_Brand_input()

        if not Brand_id.isdigit():
            self.view.show_message("Error: 'Brand_id' must be a number.")
            return

        if len(Name) > 30:
            self.view.show_message("Error: The value of the 'Name' column exceeds 30 characters.")
            return

        result_message = self.model.add_Brand(Brand_id, Name)
        self.view.show_message(result_message)

    def view_Brand(self):
        Brands = self.model.get_all_Brand()
        self.view.show_Brand(Brands)

    def update_Brand(self):
        Brand_id = self.view.get_id()
        if not Brand_id.isdigit():
            self.view.show_message("Error: 'Brand_id' must be a number.")
            return
        Name = self.view.get_Brand_update()
        if len(Name) > 30:
            self.view.show_message("Error: The value of the 'Name' column exceeds 30 characters.")
            return
        result_message =  self.model.update_Brand(Brand_id, Name)
        self.view.show_message(result_message)

    def delete_Brand(self):
        Brand_id = self.view.get_id()
        if not Brand_id.isdigit():
            self.view.show_message("Error: 'Brand_id' must be a number.")
            return

        result_message =  self.model.delete_Brand(Brand_id)
        self.view.show_message(result_message)
