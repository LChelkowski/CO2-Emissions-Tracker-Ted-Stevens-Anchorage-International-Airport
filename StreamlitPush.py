import os
import subprocess
import time

# Start the Panel server in a subprocess
panel_process = subprocess.Popen(
    ['panel', 'serve', 'Panel.py', '--address=0.0.0.0', '--port=8501']
)

# Give the Panel server some time to start
time.sleep(5)

# Notify Streamlit to load the Panel app
st.markdown(f'<iframe src="http://localhost:8501" width="100%" height="800px"></iframe>', unsafe_allow_html=True)
