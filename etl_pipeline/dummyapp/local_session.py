from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey,Float, DateTime
from .connections import conn_string_local

Base = declarative_base()

class SalesInvoiceDetail(Base):
    __tablename__ = "sales_invoice_details"
    id= Column(Integer, primary_key=True, autoincrement=True)
    sales_invoice_id = Column(Integer)
    product_id = Column(Integer, primary_key=True)
    quantity = Column(Integer, nullable=False)
    bill_amount = Column(Float, nullable=False)

class SalesReturn(Base):
    __tablename__ = "sales_return"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, nullable=False)
    billing_user_id = Column(Integer, nullable=False)
    customer_id = Column(Integer, nullable=False)
    sales_invoice_id = Column(Integer, nullable=False)
    bill_amount = Column(Float, nullable=False)
    created_at = Column(DateTime, nullable=False)

class SalesReturnDetail(Base):
    __tablename__ = "sales_return_details"
    id = Column(Integer, primary_key=True, autoincrement=True)
    sales_invoice_detail_id = Column(Integer)
    sales_invoice_id = Column(Integer, nullable=False)
    product_id = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    bill_amount = Column(Float, nullable=False)

class AdvanceSalesInvoice(Base):
    __tablename__ = "advance_sales_invoices"    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sales_invoice_draft_id = Column(Integer,nullable=True)
    store_id = Column(Integer, nullable=False)
    status = Column(String, nullable=False)
    is_urgent_order = Column(Boolean, default=False)
    total_amount = Column(Float, nullable=False)

class Product(Base):
    __tablename__ = "products"
    ws_code = Column(Integer, primary_key=True)
    product_name = Column(String)
    category = Column(String)
    is_active = Column(Boolean, default=False)

class stores(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True)
    store_name = Column(String)
    is_active = Column(Boolean, default=False)

class users(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)

class sales_invoices(Base):
    __tablename__ = "sales_invoices"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer)
    billing_user_id = Column(Integer)
    customer_id = Column(Integer,nullable=True)
    prepaid_amount = Column(Integer,nullable=True)
    #sales_invoice_draft_id = Column(Integer,nullable=True)
    total_bill_amount = Column(Float,nullable=True)
    created_at = Column(String,nullable=True)

local_engine = create_engine(conn_string_local())

Base.metadata.create_all(local_engine)

LocalSession = sessionmaker(autocommit=False, autoflush=False, bind=local_engine)


def create_local_session():
    return LocalSession()





# class SalesInvoiceDetail(Base):
#     __tablename__ = "sales_invoice_details"
#     sales_invoice_id = Column(Integer, ForeignKey("sales_invoices.id"), primary_key=True)
#     product_id = Column(Integer, ForeignKey("products.ws_code"), primary_key=True)
#     quantity = Column(Integer, nullable=False)
#     bill_amount = Column(Float, nullable=False)

# class SalesReturn(Base):
#     __tablename__ = "sales_return"
#     id = Column(Integer, primary_key=True)
#     store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
#     billing_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
#     customer_id = Column(Integer, nullable=False)
#     sales_invoice_id = Column(Integer, ForeignKey("sales_invoices.id"), nullable=False)
#     bill_amount = Column(Float, nullable=False)
#     created_at = Column(DateTime, nullable=False)

# class SalesReturnDetail(Base):
#     __tablename__ = "sales_return_details"
#     sales_invoice_detail_id = Column(Integer, primary_key=True)
#     sales_invoice_id = Column(Integer, ForeignKey("sales_invoices.id"), nullable=False)
#     product_id = Column(Integer, ForeignKey("products.ws_code"), nullable=False)
#     quantity = Column(Integer, nullable=False)
#     bill_amount = Column(Float, nullable=False)

# class AdvanceSalesInvoice(Base):
#     __tablename__ = "advance_sales_invoices"    
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     sales_invoice_draft_id = Column(Integer,nullable=True)
#     store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
#     status = Column(String, nullable=False)
#     is_urgent_order = Column(Boolean, default=False)
#     total_amount = Column(Float, nullable=False)

# class Product(Base):
#     __tablename__ = "products"
#     ws_code = Column(Integer, primary_key=True)
#     product_name = Column(String)
#     category = Column(String)
#     is_active = Column(Boolean, default=False)

# class stores(Base):
#     __tablename__ = "stores"
#     id = Column(Integer, primary_key=True)
#     store_name = Column(String)
#     is_active = Column(Boolean, default=False)

# class users(Base):
#     __tablename__ = "users"
#     id = Column(Integer, primary_key=True)
#     name = Column(String)

# class sales_invoices(Base):
#     __tablename__ = "sales_invoices"
#     id = Column(Integer, primary_key=True)
#     store_id = Column(Integer, ForeignKey("stores.id"))
#     billing_user_id = Column(Integer, ForeignKey("users.id"))
#     customer_id = Column(Integer,nullable=True)
#     prepaid_amount = Column(Integer,nullable=True)
#     #sales_invoice_draft_id = Column(Integer,nullable=True)
#     total_bill_amount = Column(Float,nullable=True)
#     created_at = Column(String,nullable=True)


