import time

from Controller_Brand import Controller_Brand
from Controller_Category import Controller_Category
from Controller_Product import Controller_Product
from Controller_SubCategory import Controller_SubCategory
from model import Model
from view import View


class Controller:
    def __init__(self):
        self.model = Model()
        self.view = View()
        self.Controller_Category = Controller_Category()
        self.Controller_SubCategory = Controller_SubCategory()
        self.Controller_Brand = Controller_Brand()
        self.Controller_Product= Controller_Product()



    def run(self):
        while True:
            choice = self.show_menu()
            if choice == '1':
                self.Controller_Category.run()
            if choice == '2':
                self.Controller_SubCategory.run()
            if choice == '3':
                self.Controller_Brand.run()
            if choice == '4':
                self.Controller_Product.run()
            if choice == '5':
                self.Generate()
            if choice == '6':
                self.Search()
            if choice == '7':
                self.Reset()
            if choice == '8':
                break

    def show_menu(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Category")
        self.view.show_message("2. SubCategory")
        self.view.show_message("3. Brand")
        self.view.show_message("4. Product")
        self.view.show_message("5. Generate")
        self.view.show_message("6. Search")
        self.view.show_message("7. Reset all")
        self.view.show_message("8. Quit")
        return input("Enter your choice: ")

    def show_submenu(self):
        self.view.show_message("\nSubMenu:")
        self.view.show_message("1. Category")
        self.view.show_message("2. SubCategory")
        self.view.show_message("3. Brand")
        self.view.show_message("4. Product")
        self.view.show_message("5. SubCategory_Brand")
        self.view.show_message("6. Quit")
        return input("Enter your choice: ")

    def show_searchmenu(self):
        self.view.show_message("\nSearchMenu:")
        self.view.show_message("1. Print the name of the brand that has the largest number of products with these characteristics: \"Energy consumption\" less than 20 and \"Weight\" less than 30.")
        self.view.show_message("2. Print the name of the subcategory that has the brand with products with the highest height and length.")
        self.view.show_message("3. Print the category in which the subcategory contains the brands that have products of yellow color.")
        self.view.show_message("4. Quit")
        return input("Enter your choice: ")

    def Generate(self):
        while True:
            self.view.show_message("Select the amount of data to generate.")
            number = self.view.get_number_for_generation()
            if not number.isdigit():
                self.view.show_message("Error: 'number' must be a number.")
                return
            choice = self.show_submenu()
            if choice == '1':
                 self.model.generate_category(number)
            elif choice == '2':
                self.model.generate_subcategory(number)
            elif choice == '3':
                self.model.generate_brand(number)
            elif choice == '4':
                self.model.generate_product(number)
            elif choice == '5':
                self.model.generate_subcategory_brand(number)
            elif choice == '6':
                break
            self.view.show_message("Randomize data added successfully!")


    def Reset(self):
        self.model.reset()
        self.view.show_message("Reset complete successfully!")

    def Search(self):
        while True:
            self.view.show_message("Enter the number of the request to search.")
            choice = self.show_searchmenu()
            if not choice.isdigit():
                self.view.show_message("Error: 'number' must be a number.")
                return
            start_time = time.time()
            if choice == '1':
                 search_results , column_description = self.model.search_request1()
            elif choice == '2':
                search_results, column_description = self.model.search_request2()
            elif choice == '3':
                search_results, column_description = self.model.search_request3()
            elif choice == '4':
                break

            end_time = time.time()
            execution_time = (end_time - start_time) * 1000

            self.view.format_and_print_results(search_results, column_description)
            self.view.show_message(f"Search request successfully done! Execution time: {execution_time:.2f} milliseconds")
