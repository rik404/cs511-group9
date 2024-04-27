import matplotlib.pyplot as plt
import numpy as np
x_labels = ['SF1', 'SF2', 'SF3', 'SF4', 'SF5']
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4', 'Query 5']
execution_times = {
    'SF1': [365,346,325,364,329],
    'SF2': [789,850,736,898,765],
    'SF3': [1091,1120,1150,1164,1138],
    'SF4': [1675,1654,1633,1699,1611],
    'SF5': [2189,2171,2234,2270,2187]
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
ax.set_title('Query Execution Times for Redis', fontweight="bold")
ax.set_xticks(np.arange(len(x_labels)) + bar_width * (len(queries) - 1) / 2)
ax.set_xticklabels(x_labels)
ax.legend(handles, queries)
plt.tight_layout()
plt.show()