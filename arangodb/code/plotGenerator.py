import matplotlib.pyplot as plt
import numpy as np
x_labels = ['SF1', 'SF2', 'SF3', 'SF4', 'SF5']
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4', 'Query 5']
execution_times = {
    'SF1': [23.319, 12.965, 14.841, 8.358, 12.823],
    'SF2': [42.637, 27.714, 31.964, 24.455, 32.329],
    'SF3': [68.698, 50.568, 61.1, 52.686, 70.968],
    'SF4': [100.918, 114.878, 117.197, 91.496, 99.707],
    'SF5': [131.7655, 143.19, 149.79, 112.1288, 136.224]
}
fig, ax = plt.subplots()
bar_width = 0.15
colors = ['blue', 'orange', 'green', 'red', 'purple']
handles = []
for i, query in enumerate(queries):
    for j, sf_label in enumerate(x_labels):
        x = j + i * bar_width
        bar = ax.bar(x, execution_times[sf_label][i], width=bar_width, color=colors[i])
    handles.append(bar)
ax.set_xlabel('Datasets', fontweight="bold")
ax.set_ylabel('Time taken (seconds)', fontweight="bold")
ax.set_title('Query Execution Times for ArangoDB', fontweight="bold")
ax.set_xticks(np.arange(len(x_labels)) + bar_width * (len(queries) - 1) / 2)
ax.set_xticklabels(x_labels)
ax.legend(handles, queries)
plt.tight_layout()
plt.show()
