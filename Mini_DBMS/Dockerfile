FROM eclipse-temurin:17

WORKDIR /app

COPY . .

RUN javac -cp ".:lib/*" *.java

EXPOSE 3000

CMD ["java", "-cp", ".:lib/*", "MainServer"]