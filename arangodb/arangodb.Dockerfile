
# Use the official ArangoDB image from Docker Hub
FROM arangodb

# Set environment variables for ArangoDB configuration
ENV ARANGO_ROOT_PASSWORD cs511grp9

RUN mkdir -p /data/db 
# Expose the ArangoDB port
EXPOSE 8529

# Command to start ArangoDB when the container is launched
CMD ["arangod", "--server.endpoint", "tcp://0.0.0.0:8529", "--server.authentication", "false"]
