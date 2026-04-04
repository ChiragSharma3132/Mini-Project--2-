FROM eclipse-temurin:17

WORKDIR /app

COPY . .

RUN find backend -name "*.java" | xargs javac -cp "lib/*"

CMD ["java", "-cp", "lib/*:backend", "backend.MainServer"]