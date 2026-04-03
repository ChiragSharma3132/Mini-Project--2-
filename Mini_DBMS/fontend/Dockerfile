# Java install karega
FROM openjdk:17

# Working directory set
WORKDIR /app

# Project ke saare files copy
COPY . .

# Java files compile karega
RUN javac -cp ".:lib/*" *.java

# Port expose (Railway ke liye)
EXPOSE 3000

# Server start karega
CMD ["java", "-cp", ".:lib/*", "MainServer"]