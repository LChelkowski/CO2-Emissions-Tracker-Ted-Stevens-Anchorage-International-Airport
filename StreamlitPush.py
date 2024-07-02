import subprocess
import streamlit as st
import time

# Function to start the Panel server
def start_panel_server():
    process = subprocess.Popen(
        ['panel', 'serve', 'app.py', '--address=0.0.0.0', '--port=8501'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return process

# Start the Panel server
panel_process = start_panel_server()

# Wait for the Panel server to start
time.sleep(5)

# Check if the server is running
try:
    st.markdown(f'<iframe src="http://localhost:8501" width="100%" height="800px"></iframe>', unsafe_allow_html=True)
except Exception as e:
    st.write("Error loading the Panel app:", e)

# Ensure the Panel server stops when the Streamlit app stops
import atexit
atexit.register(lambda: panel_process.kill())
