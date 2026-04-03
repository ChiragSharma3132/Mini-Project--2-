FROM eclipse-temurin:17

WORKDIR /app

COPY . .

RUN find . -name "*.java" > sources.txt && javac -cp ".:lib/*" @sources.txt

EXPOSE 3000

CMD ["java", "-cp", ".:lib/*:backend/dbengine", "dbengine.MainServer"]