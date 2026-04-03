FROM eclipse-temurin:17

WORKDIR /app

COPY . .

RUN javac -cp ".:lib/*" backend/dbengine/*.java

EXPOSE 3000

CMD ["java", "-cp", ".:lib/*:backend/dbengine", "dbengine.MainServer"]