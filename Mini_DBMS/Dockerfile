FROM eclipse-temurin:17

WORKDIR /app

COPY . .

RUN javac -cp "lib/*" backend/dbengine/*.java

CMD ["java", "-cp", "lib/*:backend", "backend.MainServer"]