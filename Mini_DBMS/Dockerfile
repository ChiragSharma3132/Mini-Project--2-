FROM eclipse-temurin:17

WORKDIR /app

COPY . .

# FIXED classpath
RUN javac -cp "lib/*:." $(find backend -name "*.java")

# run
CMD ["java", "-cp", "lib/*:.", "backend.dbengine.MainServer"]