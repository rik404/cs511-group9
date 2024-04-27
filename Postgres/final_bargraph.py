import matplotlib.pyplot as plt
import numpy as np
x = [1, 2, 3, 4, 5]
y_labels = ['sf1', 'sf2', 'sf3', 'sf4', 'sf5']
y_ticks = np.arange(len(y_labels))
bar_width = 0.15
postgreSQL_data = [220,375,857,1,1]
mongoDB_data = [721,1215,1701,2275,2810]
redis_data = [376,682,1006,1,1]
arangoDB_data = [2249,3964,1,1,1]
apacheKudu_data = [42,95,1,1,1]
plt.bar([val - 2*bar_width for val in x], postgreSQL_data, color='red', width=bar_width, label='PostgreSQL')
plt.bar([val - bar_width for val in x], mongoDB_data, color='blue', width=bar_width, label='MongoDB')
plt.bar(x, redis_data, color='black', width=bar_width, label='Redis')
plt.bar([val + bar_width for val in x], arangoDB_data, color='green', width=bar_width, label='ArangoDB')
plt.bar([val + 2*bar_width for val in x], apacheKudu_data, color='brown', width=bar_width, label='Apache Kudu')
plt.xlabel('Datasets')
plt.ylabel('Time (In Seconds)')
plt.xticks(x, y_labels)
plt.legend()
plt.show()