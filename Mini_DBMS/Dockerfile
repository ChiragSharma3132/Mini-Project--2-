FROM openjdk:17-jdk-slim

WORKDIR /app

COPY . .

RUN javac -cp ".:lib/*" *.java

EXPOSE 3000

CMD ["java", "-cp", ".:lib/*", "MainServer"]