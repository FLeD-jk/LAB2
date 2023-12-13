from model import Model
from view import View
class Controller_Product:
    def __init__(self):
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            choice = self.show_submenu()
            if choice == '1':
                self.add_Product()
            elif choice == '2':
                self.view_Product()
            elif choice == '3':
                self.update_Product()
            elif choice == '4':
                self.delete_Product()
            elif choice == '5':
                break

    def show_submenu(self):
        self.view.show_message("\nSub Menu Product :")
        self.view.show_message("1. Add Product")
        self.view.show_message("2. View Product")
        self.view.show_message("3. Update Product")
        self.view.show_message("4. Delete Product")
        self.view.show_message("5. Quit")
        return input("Enter your choice: ")

    def add_Product(self):
        Product_id, Name, Color, Width, Height, Deepth,  Weight, Energy_consumption, Brand_id = self.view.get_Product_input()

        if not (Product_id.isdigit() or Width.isdigit() or Height.isdigit() or Deepth.isdigit() or Weight.isdigit() or Energy_consumption.isdigit() or Brand_id.isdigit()):
            self.view.show_message("Error: 'Product_id/Width/Height/Deepth/Weight/Energy_consumption/Brand_id' must be a number.")
            return
        if (len(Name) > 30 or len(Color) > 30):
            self.view.show_message("Error: The value of the 'Name' column exceeds 30 characters.")
            return

        result_message = self.model.add_Product(Product_id, Name, Color, Width, Height, Deepth,  Weight, Energy_consumption, Brand_id)
        self.view.show_message(result_message)

    def view_Product(self):
        Products = self.model.get_all_Product()
        self.view.show_Product(Products)

    def update_Product(self):
        Product_id = self.view.get_id()
        if not Product_id.isdigit():
            self.view.show_message("Error: 'Product_id' must be a number.")
            return
        Name, Color, Width, Height, Deepth,  Weight, Energy_consumption, Brand_id = self.view.get_Product_update()
        if not (Width.isdigit() or Height.isdigit() or Deepth.isdigit() or Weight.isdigit() or Energy_consumption.isdigit() or Brand_id.isdigit()):
            self.view.show_message(
                "Error: 'Product_id/Width/Height/Deepth/Weight/Energy_consumption/Brand_id' must be a number.")
            return
        if (len(Name) > 30 or len(Color) > 30):
            self.view.show_message("Error: The value of the 'Name' column exceeds 30 characters.")
            return
        result_message =  self.model.update_Product(Product_id, Name, Color, Width, Height, Deepth,  Weight, Energy_consumption, Brand_id)
        self.view.show_message(result_message)

    def delete_Product(self):
        Product_id = self.view.get_id()
        if not Product_id.isdigit():
            self.view.show_message("Error: 'Product_id' must be a number.")
            return

        result_message = self.model.delete_Product(Product_id)
        self.view.show_message(result_message)