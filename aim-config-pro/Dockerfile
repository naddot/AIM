FROM node:20-slim

WORKDIR /app

# Copy package files
COPY package.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY . .

# Build the React app and compile server.ts
RUN npm run build

# Expose port
ENV PORT=8080
ENV USE_GCLOUD_CLI=false
EXPOSE 8080

# Start the server
CMD ["npm", "start"]
