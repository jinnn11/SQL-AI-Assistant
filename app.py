import streamlit as st
import psycopg2
import pandas as pd
import google.generativeai as genai
import os

# --- PAGE CONFIG ---
st.set_page_config(page_title="SQL AI Assistant", page_icon="🤖", layout="wide")

# --- SECURITY: LOAD SECRETS ---
try:
    DATABASE_URL = st.secrets["DATABASE_URL"]
    GEMINI_API_KEY = st.secrets["GEMINI_API_KEY"]
    APP_PASSWORD = st.secrets["APP_PASSWORD"]
except (FileNotFoundError, KeyError):
    st.error("Secrets not found. Please add them to .streamlit/secrets.toml (local) or Streamlit Cloud Secrets.")
    st.stop()

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)

# --- LOGIN SYSTEM ---
def check_password():
    """Returns `True` if the user had the correct password."""
    def password_entered():
        if st.session_state["password"] == APP_PASSWORD:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # Centered login box
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.text_input("Enter Password:", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.text_input("Enter Password:", type="password", on_change=password_entered, key="password")
            st.error("😕 Password incorrect")
        return False
    else:
        return True

# --- MAIN APP ---
if check_password():
    
    # --- SIDEBAR: DATABASE INFO ---
    with st.sidebar:
        st.header("📂 Database Schema")
        st.markdown("""
        **Context:** This is a Sales Database for a global retail company.
        
        **Tables Available:**
        
        🌍 **Geography**
        - `Region` (Region names)
        - `Country` (Linked to Regions)
        
        👤 **Customers**
        - `Customer` (Names, Address, City)
        
        📦 **Inventory**
        - `ProductCategory` (Category names)
        - `Product` (Items & Unit Prices)
        
        🛒 **Transactions**
        - `OrderDetail` (Who bought what, when, and how much)
        
        ---
        **💡 Example Questions:**
        - *Rank the countries by total sales amount.*
        - *Who are the top 5 customers?*
        - *What is the total revenue for the 'Beverages' category?*
        """)

    # --- MAIN CONTENT AREA ---
    st.title("🤖 SQL AI Assistant")
    st.markdown("Enter your question below, and I will generate the SQL and fetch the data for you.")

    user_question = st.text_area("Question:", "Rank the countries by total sales amount.", height=100)

    if st.button("Generate & Run Query", type="primary"):
        with st.spinner("Thinking..."):
            try:
                # 1. DEFINE SCHEMA FOR AI
                schema_context = """
                You are a PostgreSQL expert. Convert the user's question into a SQL query.
                
                Database Schema:
                - Region (RegionID, Region)
                - Country (CountryID, Country, RegionID)
                - Customer (CustomerID, FirstName, LastName, Address, City, CountryID)
                - ProductCategory (ProductCategoryID, ProductCategory, ProductCategoryDescription)
                - Product (ProductID, ProductName, ProductUnitPrice, ProductCategoryID)
                - OrderDetail (OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered)
                
                IMPORTANT RULES:
                1. Use PostgreSQL syntax.
                2. If calculating sums/averages involving money, cast to NUMERIC before rounding. Example: ROUND(SUM(...)::NUMERIC, 2)
                3. Return ONLY the SQL query. No markdown formatting (no ```sql), just the raw text.
                """

                # 2. ASK GEMINI (Fixed Model Name)
                model = genai.GenerativeModel('gemini-2.5-flash')
                prompt = f"{schema_context}\n\nQuestion: {user_question}"
                
                response = model.generate_content(prompt)
                
                # Clean up response
                sql_query = response.text.replace("```sql", "").replace("```", "").strip()
                
                # Layout: Split Query and Results
                st.subheader("Generated SQL Query:")
                st.code(sql_query, language="sql")

                # 3. EXECUTE ON RENDER DATABASE
                conn = psycopg2.connect(DATABASE_URL)
                df = pd.read_sql_query(sql_query, conn)
                conn.close()

                # 4. SHOW RESULTS
                st.subheader("Query Results:")
                st.dataframe(df, use_container_width=True)

            except Exception as e:
                st.error(f"An error occurred: {e}")