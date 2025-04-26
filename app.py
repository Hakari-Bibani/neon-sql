import streamlit as st
import pandas as pd
from handle import DatabaseHandler

st.set_page_config(page_title="Neon PostgreSQL Data Entry", page_icon="🖥️")

# Initialize database handler
@st.cache_resource
def init_db():
    return DatabaseHandler(st.secrets["postgres"]["connection_string"])

db = init_db()

# App title and description
st.title("📝 Neon PostgreSQL Data Entry App")
st.write("Add data to your Neon PostgreSQL database - single records or bulk CSV uploads")

# Tab interface
tab1, tab2 = st.tabs(["Single Entry", "Bulk CSV Upload"])

with tab1:
    st.header("Add Single Record")
    with st.form("single_entry_form"):
        name = st.text_input("Name")
        age = st.number_input("Age", min_value=0, max_value=120, step=1)
        status = st.selectbox("Status", ["Active", "Inactive", "Pending"])
        
        submitted = st.form_submit_button("Submit")
        if submitted:
            if name and age and status:
                db.insert_record(name, age, status)
                st.success("Record added successfully!")
            else:
                st.error("Please fill all fields")

with tab2:
    st.header("Bulk Upload via CSV")
    st.write("Upload a CSV file with columns: name, age, status")
    
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            # Validate columns
            if not all(col in df.columns for col in ["name", "age", "status"]):
                st.error("CSV must contain columns: name, age, status")
            else:
                st.write("Preview:")
                st.dataframe(df.head())
                
                if st.button("Upload to Database"):
                    success_count = db.bulk_insert(df)
                    st.success(f"Successfully uploaded {success_count} records!")
        except Exception as e:
            st.error(f"Error processing CSV: {e}")

# Display current data
if st.button("Show Current Data"):
    data = db.fetch_all()
    if data:
        st.dataframe(pd.DataFrame(data, columns=["name", "age", "status"]))
    else:
        st.info("No data found in the database")
