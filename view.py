class View:
    def show_Category(self, Category):
        print("Categories:")
        for category in Category:
            print(f"Category ID: {category[0]}, Name: {category[1]}")
    def get_Category_input(self):
        Category_id = input("Enter Category id: ")
        Name = input("Enter name: ")
        return Category_id, Name

    def get_Category_update(self):
        Name = input("Enter name: ")
        return Name

    def get_SubCategory_input(self):
        SubCategory_id = input("Enter SubCategory id: ")
        Name = input("Enter name: ")
        Category_id = input("Enter Category id: ")
        return SubCategory_id, Name , Category_id

    def show_SubCategory(self, SubCategory):
        print("SubCategories:")
        for subcategory in SubCategory:
            print(f"SubCategory ID: {subcategory[0]}, Name: {subcategory[1]}, Category ID: {subcategory[2]}")


    def get_SubCategory_update(self):
        Name = input("Enter name: ")
        Category_id = input("Enter Category id: ")
        return Name ,Category_id


    def show_Brand(self, Brand):
        print("Brand:")
        for brand in Brand:
            print(f"Brand ID: {brand[0]}, Name: {brand[1]}")
    def get_Brand_input(self):
        Brand_id = input("Enter Brand id: ")
        Name = input("Enter name: ")
        return Brand_id, Name

    def get_Brand_update(self):
        Name = input("Enter name: ")
        return Name


    def show_Product(self, Product):
        print("Product:")
        for product in Product:
            print(f"Product ID: {product[0]}, Name: {product[1]}, Color: {product[2]}, Width: {product[3]}, Height: {product[4]}, Deepth: {product[5]}, Weight: {product[6]}, Energy consumption: {product[7]}, Brand_id: {product[8]}")
    def get_Product_input(self):
        Product_id = input("Enter Product id: ")
        Name = input("Enter name: ")
        Color = input("Enter Color : ")
        Width = input("Enter Width: ")
        Height = input("Enter Height: ")
        Deepth = input("Enter Deepth: ")
        Weight = input("Enter Weight: ")
        Energy_consumption  = input("Enter Energy consumption: ")
        Brand_id = input("Enter Brand id: ")
        return Product_id, Name, Color, Width, Height, Deepth,  Weight, Energy_consumption, Brand_id

    def get_Product_update(self):
        Name = input("Enter name: ")
        Color = input("Enter Color : ")
        Width = input("Enter Width: ")
        Height = input("Enter Height: ")
        Deepth = input("Enter Deepth: ")
        Weight = input("Enter Weight: ")
        Energy_consumption = input("Enter Energy consumption: ")
        Brand_id = input("Enter Brand id: ")
        return  Name, Color, Width, Height, Deepth, Weight, Energy_consumption, Brand_id


    def get_id(self):
        return input("Enter ID: ")

    def get_number_for_generation(self):
        return input("Enter number: ")

    def format_and_print_results(self, results,column_description):
        if not results:
            self.show_message("No results found.")
            return

        column_names = [description[0] for description in column_description]
        max_lengths = [len(name) for name in column_names]
        for row in results:
            max_lengths = [max(max_lengths[i], len(str(value)) if value is not None else 0) for i, value in
                           enumerate(row)]

        header = "|".join([name.ljust(max_lengths[i]) for i, name in enumerate(column_names)])
        self.show_message(header)

        for row in results:
            formatted_row = "|".join(
                [str(value).ljust(max_lengths[i]) if value is not None else 'None'.ljust(max_lengths[i]) for i, value in
                 enumerate(row)])
            self.show_message(formatted_row)

    def show_message(self, message):
        print(message)

