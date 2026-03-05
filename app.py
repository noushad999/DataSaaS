import streamlit as st
import pandas as pd
from openai import OpenAI
from sqlalchemy import create_engine, inspect, text
import os

# ==========================================
# 0. Basic Authentication Layer (SaaS Simulation)
# ==========================================
def check_password():
    """Returns `True` if the user had the correct password."""
    def password_entered():
        if st.session_state["password"] == "admin123": # ডেমো পাসওয়ার্ড
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Enter SaaS Password (hint: admin123)", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Enter SaaS Password", type="password", on_change=password_entered, key="password")
        st.error("😕 Password incorrect")
        return False
    return True

# ==========================================
# 1. NVIDIA API & Security Firewall Configuration
# ==========================================
NVIDIA_API_KEY = "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"
MODEL_NAME = "meta/llama-3.1-70b-instruct"

client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=NVIDIA_API_KEY)

def is_safe_query(query):
    """Firewall: Prevent accidental or malicious database modification"""
    forbidden_keywords = ['drop', 'delete', 'update', 'insert', 'alter', 'truncate', 'grant', 'revoke', 'commit']
    query_lower = query.lower()
    for word in forbidden_keywords:
        # Check if the forbidden word exists as a standalone word
        if f" {word} " in f" {query_lower} " or query_lower.startswith(f"{word} "):
            return False, f"🚨 Security Alert: '{word.upper()}' command is blocked for safety!"
    return True, "Safe"

# ==========================================
# 2. Dynamic Schema Extractor
# ==========================================
def extract_schema(engine):
    inspector = inspect(engine)
    schema_info = []
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        col_details = [f"{col['name']} ({col['type']})" for col in columns]
        
        fks = inspector.get_foreign_keys(table_name)
        fk_details = [f"FK: {fk['constrained_columns'][0]}->{fk['referred_table']}.{fk['referred_columns'][0]}" for fk in fks]
            
        table_desc = f"Table: {table_name}\nColumns: {', '.join(col_details)}"
        if fk_details: table_desc += f"\nRelationships: {', '.join(fk_details)}"
        schema_info.append(table_desc)
    return "\n\n".join(schema_info)

# ==========================================
# 3. LLM Caller
# ==========================================
def call_llm(prompt, system_message, temp=0.0):
    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt}
            ],
            temperature=temp, max_tokens=1000
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {str(e)}"

# ==========================================
# 4. Agentic Workflow
# ==========================================
def process_query(user_question, engine, schema_context, dialect):
    system_prompt = f"""
    You are an expert Data Analyst. Convert natural language to strictly READ-ONLY (SELECT) {dialect} SQL queries.
    Schema:
    {schema_context}
    
    Rules: 
    1. Return ONLY the executable SQL query. No explanations. No Markdown. MUST BE A SELECT QUERY.
    2. ALWAYS use unique aliases for columns in your SELECT statement (e.g., Track.Name AS TrackName, Artist.Name AS ArtistName) to prevent duplicate column names.
    3. Use proper JOINs and aggregate functions where necessary.
    """
    
    generated_sql = call_llm(user_question, system_prompt, temp=0.0)
    generated_sql = generated_sql.replace("```sql", "").replace("```", "").replace(";", "").strip()
    
    # 🔥 Firewall Check
    is_safe, msg = is_safe_query(generated_sql)
    if not is_safe:
        st.error(msg)
        return None, generated_sql

    for attempt in range(3):
        try:
            with engine.connect() as conn:
                df = pd.read_sql_query(text(generated_sql), conn)
                
                # 🛡️ Safety Net: Handle duplicate column names automatically for Streamlit
                if df.columns.duplicated().any():
                    # Rename duplicate columns (e.g., Name, Name_1, Name_2)
                    cols = pd.Series(df.columns)
                    for dup in cols[cols.duplicated()].unique():
                        cols[cols[cols == dup].index.values.tolist()] = [f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))]
                    df.columns = cols
                    
            return df, generated_sql
        except Exception as e:
            error_msg = str(e)
            st.warning(f"⚠️ Self-correcting attempt {attempt+1}... (Fixing SQL logic)")
            correction_prompt = f"Query failed: {generated_sql}\nError: {error_msg}\nFix the query. Ensure ALL selected columns have UNIQUE aliases (AS alias_name). Return ONLY valid SQL."
            generated_sql = call_llm(correction_prompt, system_prompt, temp=0.0)
            generated_sql = generated_sql.replace("```sql", "").replace("```", "").replace(";", "").strip()

    return None, generated_sql

# ==========================================
# 5. UI (All-in-One Commercial Dashboard)
# ==========================================
def main():
    st.set_page_config(page_title="DataSaaS - AI Data Analyst", page_icon="🏢", layout="wide")
    
    # 🔒 Login Check
    if not check_password():
        st.stop()
        
    st.title("🏢 DataSaaS: Enterprise AI Analyst")
    st.markdown("**All-in-One Platform: Query, Analyze, and Export your Data securely.**")
    
    with st.sidebar:
        st.header("🔗 1. Connect Database")
        db_type = st.selectbox("Database Type", ["SQLite", "PostgreSQL", "MySQL"])
        
        placeholder = "sqlite:///chinook.db"
        if db_type == "PostgreSQL": placeholder = "postgresql://user:password@localhost/mydb"
            
        db_url = st.text_input("Database URI", placeholder=placeholder, type="password")
        
        if st.button("Connect & Extract Schema") and db_url:
            try:
                engine = create_engine(db_url)
                with engine.connect(): pass 
                st.session_state.engine = engine
                st.session_state.db_type = db_type
                with st.spinner("Extracting schema safely..."):
                    st.session_state.schema = extract_schema(engine)
                st.success("✅ Database Connected safely!")
            except Exception as e:
                st.error(f"Connection Failed: {e}")
                
        st.markdown("---")
        st.markdown("🛡️ **Security Check:** Active\n\n📥 **Export Module:** Active")

    if "engine" not in st.session_state or not st.session_state.engine:
        st.info("👈 Please connect a database to begin your secure session.")
        return
        
    user_question = st.text_input("💬 Ask complex questions (e.g., 'Top 5 artists by revenue'):")
    
    if st.button("Generate Insight") and user_question:
        with st.spinner("Analyzing and generating secure SQL..."):
            df, final_sql = process_query(user_question, st.session_state.engine, st.session_state.schema, st.session_state.db_type)
            
            if df is not None:
                st.success("✅ Query Executed Successfully!")
                with st.expander("🔍 Audit Log: Generated SQL"):
                    st.code(final_sql, language="sql")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("📋 Raw Data")
                    st.dataframe(df.head(50))
                    
                    # 📥 CSV Export Button
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Data as CSV",
                        data=csv,
                        file_name='data_insight.csv',
                        mime='text/csv',
                    )
                
                with col2:
                    st.subheader("💡 AI Insight")
                    
                    # 🛡️ Anti-Hallucination Prompt
                    insight_prompt = f"""
                    User asked: '{user_question}'
                    Data Result: {df.head(10).to_dict('records')}
                    
                    Rules:
                    1. If the Data Result is empty ([]), strictly reply "There is no data available for this specific query." DO NOT make up or hallucinate any data.
                    2. If data is present, write a 2-line business insight based STRICTLY on the provided Data Result.
                    """
                    
                    st.info(call_llm(insight_prompt, "You are a highly strictly factual business consultant.", temp=0.1))

                    
                    if len(df.columns) >= 2 and df.select_dtypes(include='number').shape[1] > 0:
                        st.subheader("📈 Chart")
                        try: st.bar_chart(df.set_index(df.columns[0]))
                        except: pass
            else:
                st.error("Operation failed or was blocked by Firewall.")

if __name__ == "__main__":
    main()
