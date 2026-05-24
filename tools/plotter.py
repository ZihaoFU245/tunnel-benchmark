import pandas as pd
import matplotlib.pyplot as plt

# 1. Load the telemetry data
caddy_df = pd.read_csv('caddy.csv')
nginx_df = pd.read_csv('nginx.csv')

# 2. Convert string timestamps into datetime objects
caddy_df['timestamp'] = pd.to_datetime(caddy_df['timestamp'])
nginx_df['timestamp'] = pd.to_datetime(nginx_df['timestamp'])

# 3. Normalize time by subtracting the start time (t0) for each proxy
caddy_df['elapsed_seconds'] = (caddy_df['timestamp'] - caddy_df['timestamp'].min()).dt.total_seconds()
nginx_df['elapsed_seconds'] = (nginx_df['timestamp'] - nginx_df['timestamp'].min()).dt.total_seconds()

# 4. Initialize the plot using subplots
fig, ax = plt.subplots(figsize=(10, 6))

# 5. Plot memory usage converted from KB to MB (value / 1024)
ax.plot(
    nginx_df['elapsed_seconds'], 
    nginx_df['vm_rss_kb'] / 1024, 
    label='Nginx (RSS)', 
    color='#009639', 
    linewidth=2
)
ax.plot(
    caddy_df['elapsed_seconds'], 
    caddy_df['vm_rss_kb'] / 1024, 
    label='Caddy (RSS)', 
    color='#00ADD8', 
    linewidth=2
)

# 6. Formatting and labels
ax.set_title('Normalized Memory Usage Comparison (RSS) over Time', fontsize=14, pad=15)
ax.set_xlabel('Elapsed Time (Seconds)', fontsize=12)
ax.set_ylabel('Resident Set Size (MB)', fontsize=12)
ax.legend(fontsize=11, loc='best')
ax.grid(True, linestyle='--', alpha=0.6)

# 7. Optimize layout and save the visualization
plt.tight_layout()
plt.savefig('normalized_memory_comparison.png', dpi=300)
