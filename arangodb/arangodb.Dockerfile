
# # Use the official ArangoDB image from Docker Hub
# FROM arangodb

# # Set environment variables for ArangoDB configuration
# ENV ARANGO_ROOT_PASSWORD cs511grp9

# RUN mkdir -p /data/db 
# # Expose the ArangoDB port
# EXPOSE 8529

# COPY /data/snow_date.csv /snow_date.csv
# # Command to start ArangoDB when the container is launched
# CMD ["arangod", "--server.endpoint", "tcp://0.0.0.0:8529", "--server.authentication", "false", "&","arangoimport", "--file", "'/snow_date.csv'", "--type" ,"csv", "--collection", "'users'"]


# Use the official ArangoDB image from Docker Hub
FROM arangodb

# Set environment variables for ArangoDB configuration
ENV ARANGO_ROOT_PASSWORD=cs511grp9

# Create directory for data
RUN mkdir -p /data/db 

# Expose the ArangoDB port
EXPOSE 8529

# Copy CSV file into container
# COPY data/snow_date.csv /snow_date.csv
# COPY data/snow_line_item.csv /snow_line_item.csv
# Copy the entrypoint script into the container
# COPY docker-entrypoint.sh docker-entrypoint.sh

# Grant execute permissions to the entrypoint script
# RUN chmod +x /docker-entrypoint.sh

# Set the entrypoint
# ENTRYPOINT ["sh", "docker-entrypoint.sh"]
