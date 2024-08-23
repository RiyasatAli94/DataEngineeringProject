# Use the official Python image from the Docker Hub
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Show the contents of the /app directory
RUN ls /app

# Install system dependencies (optional, if needed)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Show contents of requirements.txt
RUN if [ -f requirements.txt ]; then cat requirements.txt; fi

# Install any needed packages specified in requirements.txt with verbose logging
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt --verbose; fi

# Make port 80 available to the world outside this container
EXPOSE 80

# Run demo.py when the container launches
CMD ["python", "DataEngProgram.py"]
