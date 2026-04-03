FROM eclipse-temurin:17

WORKDIR /app

COPY . .

RUN javac -cp ".:lib/*" backend/*.java

EXPOSE 3000

CMD ["java", "-cp", ".:lib/*:backend", "backend.MainServer"]