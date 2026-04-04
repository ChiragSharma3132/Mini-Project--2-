FROM eclipse-temurin:17

WORKDIR /app

# copy all files
COPY . .

# compile with libraries
RUN javac -cp "lib/*" backend/**/*.java

# run with libraries
CMD ["java", "-cp", "lib/*:.", "backend.MainServer"]