import psycopg2
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, UniqueConstraint ,exists,Sequence,select,case,cast, insert,and_
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy import String, Date
from sqlalchemy.orm import relationship
from sqlalchemy.sql.expression import func


Base = declarative_base()

class Model:
    def __init__(self):
        engine = create_engine('postgresql://postgres:1111@localhost:5432/LAB2')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def add_Category(self, Category_id, Name):
        try:
            new_category = Category(Category_id=Category_id, Name=Name)
            self.session.add(new_category)
            self.session.commit()
            return "Category added successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def get_all_Category(self):
        categories = self.session.query(Category).all()
        return [(Category.Category_id, Category.Name) for Category in categories]

    def update_Category(self, Category_id, Name):
        try:
            update_category = self.session.query(Category).get(Category_id)
            update_category.Name = Name
            self.session.commit()
            return "Category updated successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def delete_Category(self, Category_id):
        try:
            delete_category = self.session.query(Category).get(Category_id)
            if self.session.query(exists().where(SubCategory.Category_id == Category_id)).scalar():
                return "Cannot delete category. It has references in SubCategory."
            self.session.delete(delete_category)
            self.session.commit()
            return "Category delete successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message


    def add_SubCategory(self, SubCategory_id, Name,Category_id):
        try:
            new_subcategory = SubCategory(SubCategory_id=SubCategory_id, Name=Name,Category_id=Category_id)
            self.session.add(new_subcategory)
            self.session.commit()
            return "SubCategory added successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message


    def get_all_SubCategory(self):
        subcategories = self.session.query(SubCategory).all()
        return [(SubCategory.SubCategory_id, SubCategory.Name,SubCategory.Category_id) for SubCategory in subcategories]

    def update_SubCategory(self,  SubCategory_id, Name,Category_id):
        try:
            update_subcategory = self.session.query(SubCategory).get(SubCategory_id)
            update_subcategory.Name = Name
            update_subcategory.Category_id = Category_id
            self.session.commit()
            return "SubCategory updated successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def delete_SubCategory(self, SubCategory_id):
        try:
            delete_subcategory = self.session.query(SubCategory).get(SubCategory_id)
            if self.session.query(exists().where(SubCategory_Brand.SubCategory_id == SubCategory_id)).scalar():
                return "Cannot delete subcategory. It has references in SubCategory_Brand."
            self.session.delete(delete_subcategory)
            self.session.commit()
            return "SubCategory delete successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def add_Brand(self, Brand_id, Name):
        try:
            new_brand = Brand(Brand_id=Brand_id, Name=Name)
            self.session.add(new_brand)
            self.session.commit()
            return "Brand added successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def get_all_Brand(self):
        brands = self.session.query(Brand).all()
        return [(Brand.Brand_id, Brand.Name) for Brand in brands]

    def update_Brand(self, Brand_id, Name):
        try:
            update_brand = self.session.query(Brand).get(Brand_id)
            update_brand.Name = Name
            self.session.commit()
            return "Brand updated successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def delete_Brand(self, Brand_id):
        try:
            delete_brand = self.session.query(Brand).get(Brand_id)
            if self.session.query(exists().where(SubCategory_Brand.Brand_id == Brand_id)).scalar():
                return "Cannot delete subcategory. It has references in SubCategory_Brand."
            if self.session.query(exists().where(Product.Brand_id == Brand_id)).scalar():
                return "Cannot delete brand. It has references in Product."
            self.session.delete(delete_brand)
            self.session.commit()
            return "Brand delete successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message


    def add_Product(self, Product_id, Name, Color, Width, Height, Deepth,  Weight, Energy_consumption, Brand_id):
        try:
            new_brand = Product(Product_id=Product_id, Name=Name, Color = Color, Width = Width, Height = Height,Deepth = Deepth, Weight = Weight,Energy_consumption = Energy_consumption,Brand_id = Brand_id)
            self.session.add(new_brand)
            self.session.commit()
            return "Product added successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def get_all_Product(self):
        products = self.session.query(Product).all()
        return [(Product.Product_id, Product.Name,Product.Color, Product.Width, Product.Height, Product.Deepth,Product.Weight,Product.Energy_consumption,Product.Brand_id) for Product in products]

    def update_Product(self, Product_id, Name, Color, Width, Height, Deepth,  Weight, Energy_consumption, Brand_id):
        try:
            update_product = self.session.query(Product).get(Product_id)
            update_product.Name = Name
            update_product.Color = Color
            update_product.Width = Width
            update_product.Height = Height
            update_product.Deepth = Deepth
            update_product.Weight = Weight
            update_product.Energy_consumption = Energy_consumption
            update_product.Brand_id = Brand_id
            self.session.commit()
            return "Product updated successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def delete_Product(self, Product_id):
        try:
            delete_product = self.session.query(Product).get(Product_id)
            self.session.delete(delete_product)
            self.session.commit()
            return "Product delete successfully!"
        except Exception as e:
            self.session.rollback()
            error_message = str(e)
            return "Error: " + error_message

    def generate_category(self, number):
        seq = Sequence("Category_Category_id_seq", start=0)
        for i in range(1, int(number) + 1):
            try:
                new_Category_id =select(seq.next_value()).scalar_subquery()
                new_Name = "Category" + str(self.session.execute(seq))
                new_Name = new_Name[:30]
                new_category = Category(Category_id=new_Category_id, Name=new_Name)
                self.session.add(new_category)
                self.session.commit()
            except Exception as e:
                print(f"Error: {e}")
                self.session.rollback()

    def generate_subcategory(self, number):
        seq = Sequence("SubCategory_SubCategory_id_seq", start=0)
        for i in range(1, int(number) + 1):
            try:
                new_SubCategory_id = select(seq.next_value()).scalar_subquery()
                new_Name = "SubCategory" + str(self.session.execute(seq))
                new_Name = new_Name[:30]
                new_category_id = (self.session.query(Category.Category_id).order_by(func.random()).limit(1).scalar())
                new_subcategory = SubCategory(SubCategory_id=new_SubCategory_id, Name=new_Name, Category_id=new_category_id)
                self.session.add(new_subcategory)
                self.session.commit()
            except Exception as e:
                print(f"Error: {e}")
                self.session.rollback()

    def generate_brand(self, number):
        seq = Sequence("Brand_Brand_id_seq", start=0)
        for i in range(1, int(number) + 1):
            try:
                new_Brand_id =select(seq.next_value()).scalar_subquery()
                new_Name = "Brand" + str(self.session.execute(seq))
                new_Name = new_Name[:30]
                new_Brand = Brand(Brand_id=new_Brand_id, Name=new_Name)
                self.session.add(new_Brand)
                self.session.commit()
            except Exception as e:
                print(f"Error: {e}")
                self.session.rollback()

    def generate_product(self, number):
        seq = Sequence("Product_Product_id_seq", start=0)
        for i in range(1, int(number) + 1):
            try:
                new_product_id = select(seq.next_value()).scalar_subquery()
                new_Name = "product" + str(self.session.execute(seq))
                new_Name = new_Name[:30]
                new_Color = case(
                (func.random() < 0.2, 'Red'),
                (func.random() < 0.4, 'Blue'),
                (func.random() < 0.6, 'Green'),
                (func.random() < 0.8, 'Yellow'),
                else_='Purple'
            )
                new_Width = cast(func.random() * 100 + 1, Integer)
                new_Height = cast(func.random() * 100 + 1, Integer)
                new_Deepth = cast(func.random() * 100 + 1, Integer)
                new_Weight = cast(func.random() * 100 + 1, Integer)
                new_Energy_consumption = cast(func.random() * 100 + 1, Integer)
                new_brand_id = (self.session.query(Brand.Brand_id).order_by(func.random()).limit(1).scalar())

                new_product = Product(Product_id=new_product_id, Name=new_Name, Color = new_Color, Width = new_Width, Height = new_Height,Deepth = new_Deepth, Weight = new_Weight,Energy_consumption = new_Energy_consumption,Brand_id=new_brand_id)
                self.session.add(new_product)
                self.session.commit()
            except Exception as e:
                print(f"Error: {e}")
                self.session.rollback()

    def generate_subcategory_brand(self, number):
        for i in range(1, int(number) + 1):
            try:
                new_SubCategory_id = (self.session.query(SubCategory.SubCategory_id).order_by(func.random()).limit(1).scalar())
                new_Brand_id = (self.session.query(Brand.Brand_id).order_by(func.random()).limit(1).scalar())
                new_subcategory_brand = SubCategory_Brand(SubCategory_id =new_SubCategory_id, Brand_id=new_Brand_id)
                self.session.add(new_subcategory_brand)
                self.session.commit()
            except Exception as e:
                print(f"Error: {e}")
                self.session.rollback()

    def reset(self):
        Base.metadata.drop_all(self.session.bind)

    def search_request1(self):
        answer = (select(Brand.Name.label('Brand'),Product.Name.label('Product'),Product.Energy_consumption,Product.Weight).select_from(Product).join(Brand, Product.Brand_id == Brand.Brand_id) .where(Product.Energy_consumption < 20, Product.Weight < 30).order_by(Brand.Name))
        result = self.session.execute(answer)
        return result.fetchall(), result.keys()

    def search_request2(self):

        CTE =(
            select(
                SubCategory.Name.label('SubCategoryName'),
                Brand.Name.label('BrandName'),
                Product.Name.label('ProductName'),
                Product.Width.label('ProductWidth'),
                Product.Height.label('ProductHeight')
            )
            .select_from(SubCategory)
            .join(SubCategory_Brand, SubCategory.SubCategory_id == SubCategory_Brand.SubCategory_id)
            .join(Brand, SubCategory_Brand.Brand_id == Brand.Brand_id)
            .join(Product, Brand.Brand_id == Product.Brand_id)
            .cte('filtered'))

        answer = (select(CTE).where(and_(CTE.c.ProductWidth == (select(func.max(CTE.c.ProductWidth)).select_from(CTE).as_scalar()),CTE.c.ProductHeight == select(func.max(CTE.c.ProductHeight)).select_from(CTE) .as_scalar())))

        result = self.session.execute(answer)
        return result.fetchall(), result.keys()

    def search_request3(self):
        answer = (
            select(
                Category.Name.label('Category_Name'),
                SubCategory.Name.label('SubCategory_Name'),
                Brand.Name.label('Brand_Name'),
                Product.Name.label('Product_Name'),
            )
            .select_from(Category)
            .join(SubCategory, Category.Category_id == SubCategory.Category_id)
            .join(SubCategory_Brand, SubCategory.SubCategory_id == SubCategory_Brand.SubCategory_id)
            .join(Brand, SubCategory_Brand.Brand_id == Brand.Brand_id)
            .join(Product, Brand.Brand_id == Product.Brand_id)
            .where(Product.Color == 'Yellow')
        )
        result = self.session.execute(answer)
        return result.fetchall(), result.keys()
class Category(Base):
    __tablename__ = 'Category'

    Category_id = Column(Integer, primary_key=True)
    Name = Column(String(30), unique=True, nullable=False)
    subcategories = relationship('SubCategory')

class SubCategory(Base):
    __tablename__ = 'SubCategory'

    SubCategory_id = Column(Integer, primary_key=True)
    Name = Column(String(30), unique=True, nullable=False)
    Category_id = Column(Integer, ForeignKey('Category.Category_id'))
    brands = relationship('Brand', secondary='SubCategory_Brand')

class Brand(Base):
    __tablename__ = 'Brand'

    Brand_id = Column(Integer, primary_key=True)
    Name = Column(String(30), unique=True, nullable=False)
    products = relationship('Product')

class Product(Base):
    __tablename__ = 'Product'

    Product_id = Column(Integer, primary_key=True)
    Name = Column(String(30), nullable=False)
    Color = Column(String(30), nullable=False)
    Width = Column(Integer, nullable=False)
    Height = Column(Integer, nullable=False)
    Deepth = Column(Integer, nullable=False)
    Weight = Column(Integer, nullable=False)
    Energy_consumption = Column(Integer, nullable=False)
    Brand_id = Column(Integer, ForeignKey('Brand.Brand_id'))

class SubCategory_Brand(Base):
    __tablename__ = 'SubCategory_Brand'

    SubCategory_id = Column(Integer, ForeignKey('SubCategory.SubCategory_id'), primary_key=True)
    Brand_id = Column(Integer, ForeignKey('Brand.Brand_id'), primary_key=True)


